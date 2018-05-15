package exectlyonce.bean;

/**
 * Created by Admin on 2018/5/10.
 * @author xudacheng
 */
public class KafkaOffsetBean {
    private String topic;
    private int partition;
    private Long fromOffset;
    private Long untilOffset;
    private String groupId;
    private int status;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public Long getFromOffset() {
        return fromOffset;
    }

    public void setFromOffset(Long fromOffset) {
        this.fromOffset = fromOffset;
    }

    public Long getUntilOffset() {
        return untilOffset;
    }

    public void setUntilOffset(Long untilOffset) {
        this.untilOffset = untilOffset;
    }
}
