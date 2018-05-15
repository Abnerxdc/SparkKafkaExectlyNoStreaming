package service

import java.util.Properties

import exectlyonce.dao.KafkaOffsetDao
import exectlyonce.util.KafkaUtil
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import org.apache.spark.{KafkaManager, SparkContext}

import scala.collection.JavaConversions
/**
  * Created by Admin on 2018/5/15.
  */
class OffsetService {
  /**
    * 每批次处理最少的数据量
    * 降低实时性，提高性能，减少资源的申请（即当topic中数据量大于这个值的时候才会更新到数据库，不建议太大，防止时间太久被kafka自动清理）
    * 只有更新到数据库中的偏移量才会被处理
    */
  val minOffsetDealEveryBatch = 100
  //每 批次处理的最大数据量
  val maxOffsetDealEveryBatch = 1000

  /**
    * 当数据库中没有需要处理的信息的时候执行该方法
    * 获取offset信息，并根据每批次处理的数据量，计算要处理的偏移信息，插入到数据库中，设置标志位为未处理
    * @param kafkaParams
    * @param consumerProp
    * @param topics
    */
  def noMessageInDbNeedDeal(kafkaParams : Map[String,String], consumerProp : Properties, topics : Set[String]): Unit ={
    val groupId = kafkaParams.get("groupId").get
    val km = new KafkaManager(kafkaParams)
    //获取当前偏移量
    val curTopicAndPartitionOffsetMap = km.getCurrentOffset(topics)
    val TopicPartitionList = curTopicAndPartitionOffsetMap.map(topicPartitionOffset => {
      (new TopicPartition(topicPartitionOffset._1.topic, topicPartitionOffset._1.partition))
    }).toList
    val kafkaUtil = new KafkaUtil
    import scala.collection.JavaConverters._
    //获取所有最大偏移
    val largestTopicPartitionOffsetMap = kafkaUtil.getTopicPartitionOffset(consumerProp, TopicPartitionList.asJava).asScala
    largestTopicPartitionOffsetMap.foreach(largestTopicPartitionAndOffset => {
      val topicPartition = largestTopicPartitionAndOffset._1
      val largestOffset = largestTopicPartitionAndOffset._2
      val curOffset = curTopicAndPartitionOffsetMap.get(TopicAndPartition(topicPartition.topic(), topicPartition.partition())).get
      val phaseTwoOffset = largestOffset - curOffset
      var startOffset = curOffset
      if (phaseTwoOffset > minOffsetDealEveryBatch) {
        val large = phaseTwoOffset / largestOffset
        if (large > 0) {
          for (i <- 0 to large.toInt) {
            // (这个分区有这么多偏移量需要处理)
            KafkaOffsetDao.insetTopicPartitionOffsetAndStatus(topicPartition.topic(), topicPartition.partition(), startOffset, startOffset + maxOffsetDealEveryBatch, 0, groupId)
            startOffset += maxOffsetDealEveryBatch
          }
          if (largestOffset - startOffset > minOffsetDealEveryBatch) {
            KafkaOffsetDao.insetTopicPartitionOffsetAndStatus(topicPartition.topic(), topicPartition.partition(), startOffset, maxOffsetDealEveryBatch, 0, groupId)
          }
        } else {
          KafkaOffsetDao.insetTopicPartitionOffsetAndStatus(topicPartition.topic(), topicPartition.partition(), startOffset, maxOffsetDealEveryBatch, 0, groupId)
        }
      }
    })
  }

  /**
    * 用于清理数据库中过期偏移量 / 清理程序终止时 执行到一半的偏移量
    * 系统启动的时候检测到 数据库中有 未处理/处理中 的数据 , 仅在程序启动的时候执行一次
    * @param kafkaParams
    * @param consumerProp
    * @param topics
    */
  def startWithMessageInDbClearOffset(kafkaParams : Map[String,String] , consumerProp : Properties, topics : Set[String]): Unit ={
    val groupId = kafkaParams.get("groupId").get
    val km = new KafkaManager(kafkaParams)
    //获取kafka中最早期的偏移信息
    val earliestOffset = km.getEarliestOffset(topics)
    earliestOffset.foreach(topicPartitionAndOffset =>{
      val topicAndPartition = topicPartitionAndOffset._1
      val offset = topicPartitionAndOffset._2
      //清理数据库中过期的偏移量
      KafkaOffsetDao.deleteOverdueOffset(topicAndPartition.topic,topicAndPartition.partition,offset,groupId)
      //更新数据库中过期的偏移量
      KafkaOffsetDao.updateOverdueOffset(topicAndPartition.topic,topicAndPartition.partition,offset,groupId)
      //清理上次执行到一半的偏移量
      KafkaOffsetDao.updateDealingOffsetStatus(groupId,1,0)
    })
  }

  /**
    * 程序开始后循环执行该方法
    * 处理数据库中待处理数据
    * @param sc
    * @param kafkaParams
    * @param topics
    */
  def dealMessageInDb(sc : SparkContext, consumerProp : Properties, kafkaParams : Map[String,String], topics : Set[String]): Unit ={
    val groupId = kafkaParams.get("groupId").get
    val km = new KafkaManager(kafkaParams)
    //从数据库中获取待处理数据
    var message = JavaConversions.asScalaBuffer(KafkaOffsetDao.selectOffsetNotDeal(groupId))
    //数据库中没有数据要处理辣
    if(message.isEmpty){
      //拉取偏移数据进数据库
      noMessageInDbNeedDeal(kafkaParams,consumerProp ,topics)
      //重新获取数据
      message = JavaConversions.asScalaBuffer(KafkaOffsetDao.selectOffsetNotDeal(groupId))
    }
    val offsetArray = message.map(kafkaOffsetBean =>{
      //将数据库中的数据置为处理中数据
      KafkaOffsetDao.updateOffsetStatus(kafkaOffsetBean.getTopic,kafkaOffsetBean.getPartition,kafkaOffsetBean.getFromOffset,kafkaOffsetBean.getUntilOffset,groupId,1,0)
      (OffsetRange(kafkaOffsetBean.getTopic,kafkaOffsetBean.getPartition,kafkaOffsetBean.getFromOffset,kafkaOffsetBean.getUntilOffset))
    }).toArray
    val rdd = KafkaUtils.createRDD[String,String,StringDecoder,StringDecoder](sc,kafkaParams,offsetArray)
    //rdd do something
    //rdd result
    message.foreach(kafkaOffsetBean =>{
      //将数据库中的数据 状态 置为 处理完成
      KafkaOffsetDao.updateOffsetStatus(kafkaOffsetBean.getTopic,kafkaOffsetBean.getPartition,kafkaOffsetBean.getFromOffset,kafkaOffsetBean.getUntilOffset,groupId,2,0)
      //更新zookeeper偏移信息 （只会往右更新，不会往左更新）
      km.updateZKOffsets(kafkaOffsetBean.getTopic,kafkaOffsetBean.getPartition,kafkaOffsetBean.getUntilOffset())
      //删除数据库中状态位为 已完成 的信息
      KafkaOffsetDao.deleteOffset(kafkaOffsetBean.getTopic,kafkaOffsetBean.getPartition,kafkaOffsetBean.getFromOffset,kafkaOffsetBean.getUntilOffset,groupId)
    })
    //结束
  }
}
