package monitor.azkaban.sender;

public abstract class BaseEvent {
    private String sender;
    private String project;
    private String flow;
    private String job;
    private String status;
    private int attempt;
    private long startTime;
    private long endTime;


    public String getProject() {
        return project;
    }

    public BaseEvent setProject(String project) {
        this.project = project;
        return this;
    }

    public String getFlow() {
        return flow;
    }

    public BaseEvent setFlow(String flow) {
        this.flow = flow;
        return this;
    }

    public String getJob() {
        return job;
    }

    public BaseEvent setJob(String job) {
        this.job = job;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public BaseEvent setStatus(String status) {
        this.status = status;
        return this;
    }

    public int getAttempt() {
        return attempt;
    }

    public BaseEvent setAttempt(int attempt) {
        this.attempt = attempt;
        return this;
    }


    public String getSender() {
        return sender;
    }

    public BaseEvent setSender(String sender) {
        this.sender = sender;
        return this;
    }

    public long getStartTime() {
        return startTime;
    }

    public BaseEvent setStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    public long getEndTime() {
        return endTime;
    }

    public BaseEvent setEndTime(long endTime) {
        this.endTime = endTime;
        return this;
    }

    public enum Type {
        JOBSTATUS, JOBSTART, JOBEND, FLOWSTART, FLOWEND
    }

}
