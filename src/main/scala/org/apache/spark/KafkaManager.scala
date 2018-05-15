package org.apache.spark

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}

/**
  * Created by Admin on 2018/5/7.
  */
class KafkaManager(kafkaParams: Map[String,String]) extends Serializable with Logging{
  private val kc = new KafkaCluster(kafkaParams)

  def getCurrentOffset(topics : Set[String]) = {

    try{
      val groupId = kafkaParams.get("group.id").get
      //担心偏移量过期，或者新的分区出现
      setOrUpdateOffsets(topics)
      //从zookeeper上读取offset开始消费message
      val messages = {
        val kafkaPartitionsE = kc.getPartitions(topics)
        if (kafkaPartitionsE.isLeft) throw new SparkException(s"get kafka partition failed: ${kafkaPartitionsE.left.get}")
        val kafkaPartitions = kafkaPartitionsE.right.get
        val consumerOffsetsE = kc.getConsumerOffsets(groupId, kafkaPartitions)
        if (consumerOffsetsE.isLeft) throw new SparkException(s"get kafka consumer offsets failed : ${consumerOffsetsE.left.get}")
        val consumerOffsets = consumerOffsetsE.right.get
        consumerOffsets
      }
      messages
    }catch {
      case e: Exception =>log.error("KafkaManager : getCurrentOffset fail !",e)
        null
    }
  }
  /**
    * 创建流前 根据实际消费情况更新消费offsets
    * @param topics
    */
  def setOrUpdateOffsets(topics : Set[String]): Unit = {
    try{
      val groupId = kafkaParams.get("group.id").get
      topics.foreach(topic => {
        var hasConsumed = true
        var reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        val kafkaPartitionsE = kc.getPartitions(Set(topic))
        if (kafkaPartitionsE.isLeft) throw new SparkException(s"get kafka partition failed:${kafkaPartitionsE.left.get}")
        val kafkaPartitions = kafkaPartitionsE.right.get
        val consumerOffsetsE = kc.getConsumerOffsets(groupId, kafkaPartitions)
        if (consumerOffsetsE.isLeft) hasConsumed = false
        if(!hasConsumed) {
          log.info(s"un consumed ,set auto.offset.reset = largest")
          reset = Some("largest")
        }
        if (reset.isDefined) {
          // 配置了offset 或 未有消费过
          var leaderOffsets : Map[TopicAndPartition ,LeaderOffset] = null
          var flag = true
          if(reset == Some("smallest")){
            flag = false
            val leaderOffsetE = kc.getEarliestLeaderOffsets(kafkaPartitions)
            if(leaderOffsetE.isLeft) throw new SparkException(s"get earliest leader offsets failed : ${leaderOffsetE.left.get}")
            leaderOffsets = leaderOffsetE.right.get
          }else{
            val leaderOffsetE = kc.getLatestLeaderOffsets(kafkaPartitions)
            if (leaderOffsetE.isLeft) throw new SparkException(s"get earliest leader offsets failed : ${leaderOffsetE.left.get}")
            leaderOffsets = leaderOffsetE.right.get
          }
          val auto = if (flag) "largest" else "smallest"
          val offsets = leaderOffsets.map {
            case (tp, offset) => {
              log.info(s"set consumer offset , auto = ${auto} , groupId = ${groupId} , topic = ${topic} , partition = ${tp.partition},offset = ${offset.offset}")
              (tp, offset.offset)
            }
          }
          kc.setConsumerOffsets(groupId, offsets)

        } else {
          //消费过
          /**
            * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
            * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
            * 针对这种情况，只要判断一下zk上的consumerOffsets和leaderEarliestOffsets的大小，
            * 如果consumerOffsets比leaderEarliestOffsets还小的话，说明是过时的offsets,
            * 这时把leaderEarliestOffsets更新为consumerOffsets
            */
          val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(kafkaPartitions)
          if (earliestLeaderOffsetsE.isLeft)
            throw new SparkException(s"get earliest leader offsets failed : ${earliestLeaderOffsetsE.left.get}")
          val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
          val consumerOffsets = consumerOffsetsE.right.get

          //可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets 为 earliestLeaderOffsets
          var offsets: Map[TopicAndPartition, Long] = Map()
          consumerOffsets.foreach({ case (tp, n) =>
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            if (n < earliestLeaderOffset) {
              log.info(s"reset consumer offset , groupId = ${groupId} , topic = ${topic} , partition = ${tp.partition}, offset = ${earliestLeaderOffset}")
              offsets += (tp -> earliestLeaderOffset)
            }
          })
          if (!offsets.isEmpty) {
            kc.setConsumerOffsets(groupId, offsets)
          }
        }
      })
    }catch {
      case e : Exception => log.error("KafkaManager : setOrUpdateOffsets error ! ")
    }
  }

  /**
    * 获取最早的偏移量
    * @param topics
    * @return
    */
  def getEarliestOffset(topics : Set[String] ): Map[TopicAndPartition,Long] ={
    val kafkaPartitionsE = kc.getPartitions(topics)
    if (kafkaPartitionsE.isLeft) throw new SparkException(s"get kafka partition failed:${kafkaPartitionsE.left.get}")
    val kafkaPartitions = kafkaPartitionsE.right.get
    val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(kafkaPartitions)
    if (earliestLeaderOffsetsE.isLeft)
      throw new SparkException(s"get earliest leader offsets failed : ${earliestLeaderOffsetsE.left.get}")
    val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
    val earliestOffset = earliestLeaderOffsets.map(x =>{
      (x._1->x._2.offset)
    })
    earliestOffset
  }

  /**
    * 只有 要更新的offset 在 当前offset 右边再更新 否则不更新
    * 更新 zookeeper 上的消费 offsets
    */
  def updateZKOffsets(topic: String,partition : Int , untilOffset : Long):Unit = {
    try{
      val groupId = kafkaParams.get("group.id").get
      val topicAndPartition = TopicAndPartition(topic, partition)
      val s = kc.getConsumerOffsets(groupId,Set(topicAndPartition))
      if(s.isLeft) {
        log.error(s" Error get ConsumerOffsets reasion : ${s.left.get}")
        //可能数据过期，重新更新
        setOrUpdateOffsets(Set(topic))
      }
      val curOffset = s.right.get.get(topicAndPartition).get
      if(curOffset < untilOffset){
        val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, untilOffset)))
        if (o.isLeft) {
          log.error(s"Error updating the offset to Kafka cluster: ${o.left.get}")
        }
      }else{
        println("cur offset is left than old , no changes need to do")
        log.info("cur offset is left than old , no changes need to do")
      }
    }catch {
      case e : Exception => log.error("KafkaManager : updateZKOffsets error!")
    }
  }
}
