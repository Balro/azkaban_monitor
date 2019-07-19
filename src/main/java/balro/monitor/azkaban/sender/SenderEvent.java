package balro.monitor.azkaban.sender;

public class SenderEvent extends BaseEvent {
    private String msg;
    private long execId;
    private Type type;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public long getExecId() {
        return execId;
    }

    public SenderEvent setExecId(long execId) {
        this.execId = execId;
        return this;
    }

    @Override
    public String toString() {
        return String.format("SenderEvent: execId=%s, project=%s, flow=%s, job=%s, status=%s, attemp=%s, startTime=%s, endTime=%s, msg=%s"
                , getExecId(), getProject(), getFlow(), getJob(), getStatus(), getAttempt(), getStartTime(), getEndTime(), getMsg());
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }
}
