package exectlyonce.util;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Admin on 2018/5/10.
 * @author xudacheng
 */
public class KafkaUtil {
    /**
     * 获取kafka当前分区、偏移信息
     * @param props
     */
    public Map<TopicPartition,Long> getKafkaEndOffset(Properties props,String topic){
//        props.put("bootstrap.servers","118.25.102.146:9092");
//        props.put("group.id","abner");
//        props.put("acks", "all");
//        props.put("enable.auto.commit","false");
//        props.put("retries", 0);
//        props.put("auto.offset.reset","earliest");
//        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//        String topic = "abner";
        org.apache.kafka.clients.consumer.KafkaConsumer<String,String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);
        System.out.println(consumer.partitionsFor(topic));
        List<PartitionInfo> list = consumer.partitionsFor(topic);
        List<TopicPartition> partitions = new ArrayList<>();
        for(int i=0 ;i <list.size();i++){
            PartitionInfo info = list.get(i);
            int num = info.partition();
            System.out.println(num);
            TopicPartition topicPartition = new TopicPartition(topic,num);
            partitions.add(topicPartition);
        }

        System.out.println(consumer.endOffsets(partitions));
        Map<TopicPartition,Long> endOffset = consumer.endOffsets(partitions);
        return endOffset;
    }
    public Map<TopicPartition,Long> getTopicPartitionOffset(Properties props , List<TopicPartition> topicPartitions){
//        props.put("bootstrap.servers","118.25.102.146:9092");
//        props.put("group.id","abner");
//        props.put("acks", "all");
//        props.put("enable.auto.commit","false");
//        props.put("retries", 0);
//        props.put("auto.offset.reset","earliest");
//        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        org.apache.kafka.clients.consumer.KafkaConsumer<String,String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);
//        System.out.println(consumer.endOffsets(topicPartitions));
        Map<TopicPartition,Long> endOffset = consumer.endOffsets(topicPartitions);
        return endOffset;
    }
}
