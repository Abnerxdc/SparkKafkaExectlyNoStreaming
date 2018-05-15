import java.util.Properties

import exectlyonce.util.KafkaUtil
import org.apache.kafka.common.TopicPartition
import org.apache.spark.KafkaManager


/**
  * Created by Admin on 2018/5/15.
  */
object TestUpdateOffset {
  def main(args: Array[String]): Unit = {
    val kafkaParam = Map(
      "zookeeper.connect"->"118.25.102.146:2181",
      "group.id"->"exactly-once",
      "metadata.broker.list"->"118.25.102.146:9092"
    )
    val km = new KafkaManager(kafkaParam)
//    km.updateZKOffsets("testKey",1,50)
    val s = km.getCurrentOffset(Set("testKey","abner"))
    val r = s.map(x =>{
      (new TopicPartition(x._1.topic,x._1.partition))
    }).toList
    val kc = new KafkaUtil
    import scala.collection.JavaConverters._
    val ss = kc.getTopicPartitionOffset(new Properties(),r.asJava)
    println(ss)
  }
}
