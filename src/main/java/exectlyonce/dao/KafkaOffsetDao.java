package exectlyonce.dao;



import exectlyonce.bean.KafkaOffsetBean;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Admin on 2018/5/10.
 */
public class KafkaOffsetDao {
    public static final String tbl_kafka_offset = "tbl_kafka_offset";
    //表字段
//    public static final String TblTopic = "topic";
//    public static final String TblPartition = "partition";
//    public static final String TblStartOffset = "start_offset";
//    public static final String TblEndOffset = "end_offset";
//    public static final String TblStatus = "status";

    /**
     * 状态0为待处理 状态1 为 处理中  状态2 为 处理结束
     * @param topic
     * @param partition
     * @param startOffset
     * @param endOffset
     */
    public static void insetTopicPartitionOffsetAndStatus(String topic , int partition , long startOffset , long endOffset , int status ,String groupId){
        String sql = String.format("insert into %s (`topic` ,`partition` , `startOffset` , `endOffset` , `status` , `groupId`) " +
                "values ( '%s' , '%s'  , '%s' , '%s' , '%s')",tbl_kafka_offset,topic,partition,startOffset,endOffset,status,groupId);

    }
    public static void deleteOffset(String topic , int partition , long startOffset , long endOffset ,String groupId){
        String sql = String.format("delete from %s where `topic` = '%s' , `partition` = '%s', `groupId` = '%s'  ," +
                "`startOffset` = '%s' , `endOffset` = '%s'",tbl_kafka_offset,topic,partition,groupId,startOffset,endOffset);

    }

    /**
     * 删除过期的offset
     * @param topic
     * @param partition
     * @param offset
     */
    public static void deleteOverdueOffset(String topic , int partition , long offset ,String groupId){
        String sql = String.format("delete from %s where `topic` = '%s' and `partition` = '%s'  and  `groupId` = '%s' and" +
                "`endOffset` <= '%s' ",tbl_kafka_offset,topic,partition,groupId , offset);

    }

    /**
     * 更新过期的offset
     * @param topic
     * @param partition
     * @param offset
     */
    public static void updateOverdueOffset(String topic , int partition , long offset , String groupId){
        String sql = String.format("update `startOffset` = '%s' from %s where `topic` = '%s' and `partition` = '%s' and `groupId` = '%s'  and " +
                "`startOffset` < '%s' ",offset , tbl_kafka_offset,topic,partition,groupId,offset);

    }
    /**
     * 重置状态信息
     */
    public static void updateDealingOffsetStatus(String groupId , int oldStatus , int newStatus){
        String sql = String.format("update `status` = '%s' from %s where `status` = '%s'  and `groupId` = '%s' ",
                newStatus,tbl_kafka_offset,oldStatus,groupId);

    }

    /**
     * 更新状态信息
     */
    public static void updateOffsetStatus(String topic , int partition , long startOffset , long endOffset ,String groupId , int newStatus , int oldStatus){
        String sql = String.format("update `status` = '%s' from %s where `topic` = '%s'  and `partition` = '%s' " +
                        "and `startOffset` = '%s' and `endOffset` = '%s' and `status` = '%s' and `groupId` = '%s' ",
                newStatus,tbl_kafka_offset,topic,partition,startOffset,endOffset,oldStatus,groupId);

    }
    /**
     * 查询数据库待处理的信息
     * @return
     */
    public static List<KafkaOffsetBean> selectOffsetNotDeal(String groupId){
        List<KafkaOffsetBean> list = new ArrayList<>();
        String sql = String.format("select * from %s where groupId = `groupId` and `status` = '0' order by `startOffset` limit 10 ",tbl_kafka_offset,groupId);
        return list;
    }
}
