package balro.monitor.azkaban.sender;

public class SenderEvent extends BaseEvent {
    private String msg;
    private String execId;

    public String getMsg() {
        return msg;
    }

    public SenderEvent setMsg(String msg) {
        this.msg = msg;
        return this;
    }


    public String getExecId() {
        return execId;
    }

    public SenderEvent setExecId(String execId) {
        this.execId = execId;
        return this;
    }

    @Override
    public String toString() {
        return String.format("SenderEvent: execId=%s, project=%s, flow=%s, job=%s, status=%s, attemp=%s, startTime=%s, endTime=%s, msg=%s"
                , getExecId(), getProject(), getFlow(), getJob(), getStatus(), getAttempt(), getStartTime(), getEndTime(), getMsg());
    }
}
