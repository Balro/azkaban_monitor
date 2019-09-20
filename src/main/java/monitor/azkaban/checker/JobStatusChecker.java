package monitor.azkaban.checker;

import monitor.azkaban.sender.BaseEvent;
import monitor.azkaban.sender.SenderEvent;
import monitor.azkaban.util.AzkabanMetaUtil;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class JobStatusChecker extends ArrayList<JobStatusChecker.JobStatusCheckEvent> implements Runnable {
    private static Logger LOG = Logger.getLogger(JobStatusChecker.class);

    private LinkedBlockingQueue<SenderEvent> queue;
    private BasicDataSource bds;

    private int interval;

    public static void setLogger(Logger logger) {
        LOG = logger;
    }

    public JobStatusChecker(HierarchicalConfiguration conf, LinkedBlockingQueue<SenderEvent> queue, BasicDataSource bds, int interval) {
        this.queue = queue;
        this.bds = bds;
        this.interval = interval;
        initCheckList(conf);
    }

    private void initCheckList(HierarchicalConfiguration conf) {
        conf.getList("");
        ConfigurationNode cNode = conf.getRootNode();
        for (ConfigurationNode check : cNode.getChildren()) {
            String status = conf.getString(check.getName() + ".status");
            if (status == null) continue;
            this.add(new JobStatusCheckEvent(conf.getString(check.getName() + ".project", "%")
                    , conf.getString(check.getName() + ".flow", "%")
                    , conf.getString(check.getName() + ".job", "%")
                    , status
                    , conf.getInt(check.getName() + ".attempt", 0)
                    , conf.getString(check.getName() + ".sender")));
        }
        StringBuilder sb = new StringBuilder();
        sb.append("JobStatusChecker initialized ").append(this.size()).append(" checkEvents : [");
        for (JobStatusCheckEvent ce : this) {
            sb.append(ce.toString()).append(",");
        }
        sb.append("]");
        LOG.info(sb.toString());
    }

    @Override
    public void run() {
        long lastCheckTime = System.currentTimeMillis();
        long currentCheckTime;
        while (!Thread.interrupted()) {
            currentCheckTime = System.currentTimeMillis();
            for (JobStatusCheckEvent ce : this) {
                if (ce.shouldCheck(lastCheckTime, currentCheckTime)) {
                    try {
                        List<SenderEvent> list = AzkabanMetaUtil.checkJobStatus(bds.getConnection()
                                , lastCheckTime
                                , currentCheckTime
                                , ce.getAttempt()
                                , ce.getStatus()
                                , ce.getProject()
                                , ce.getFlow()
                                , ce.getJob());
                        LOG.debug(String.format("JobStatusChecker run time range [%s, %s] on event %s, found %d events.",
                                lastCheckTime, currentCheckTime, ce.toString(), list.size()));
                        for (SenderEvent se : list) {
                            se.setType(SenderEvent.Type.JOBSTATUS);
                            se.setMsg(String.format("Job in status, execId=%d, project=%s, flow=%s, job=%s, attempt=%d, status=%s, start=%s, end=%s."
                                    , se.getExecId(), se.getProject(), se.getFlow(), se.getJob(), se.getAttempt(), se.getStatus()
                                    , new Date(se.getStartTime()), new Date(se.getEndTime())));
                            while (!queue.offer(se, interval, TimeUnit.MILLISECONDS)) {
                                LOG.warn(String.format("SenderEvent offer to dispacher failed, retry... %s", se));
                            }
                            LOG.info(String.format("Offer event to dispacher succ: %s", se));
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("JobStatusChecker interrupted.", e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        LOG.warn(null, e);
                    }
                }
            }
            lastCheckTime = currentCheckTime;
            try {
                TimeUnit.MILLISECONDS.sleep(interval);
            } catch (InterruptedException e) {
                LOG.warn("JobStatusChecker interrupted.", e);
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("JobStatusChecker stopped.");
    }

    protected class JobStatusCheckEvent extends BaseEvent implements ShouldCheckable {

        private JobStatusCheckEvent(String project, String flow, String job, String status, int attempt, String sender) {
            setProject(project);
            setFlow(flow);
            setJob(job);
            setStatus(status);
            setAttempt(attempt);
            setSender(sender);
        }

        @Override
        public boolean shouldCheck(long start, long end) {
            return true;
        }

        @Override
        public String toString() {
            return String.format("CheckEvent: project=%s, flow=%s, job=%s, status=%s, attempt=%s, sender=%s"
                    , getProject(), getFlow(), getJob(), getStatus(), getAttempt(), getSender());
        }
    }
}
