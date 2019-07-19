package balro.monitor.azkaban.checker;

import balro.monitor.azkaban.sender.BaseEvent;
import balro.monitor.azkaban.sender.SenderEvent;
import balro.monitor.azkaban.util.AzkabanMetaUtil;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class JobEndChecker extends ArrayList<JobEndChecker.JobEndCheckEvent> implements Runnable {
    private static Logger LOG = Logger.getLogger(JobEndChecker.class);

    private LinkedBlockingQueue<SenderEvent> queue;
    private BasicDataSource bds;

    private int interval;

    public static void setLogger(Logger logger) {
        LOG = logger;
    }

    public JobEndChecker(HierarchicalConfiguration conf, LinkedBlockingQueue<SenderEvent> queue, BasicDataSource bds, int interval) throws Exception {
        this.queue = queue;
        this.bds = bds;
        this.interval = interval;
        initCheckList(conf);
    }

    private void initCheckList(HierarchicalConfiguration conf) throws Exception {
        conf.getList("");
        ConfigurationNode cNode = conf.getRootNode();
        for (ConfigurationNode check : cNode.getChildren()) {
            String endtime = conf.getString(check.getName() + ".endtime");
            String job = conf.getString(check.getName() + ".job");
            if (job == null || endtime == null) continue;
            this.add(new JobEndCheckEvent(conf.getString(check.getName() + ".project")
                    , conf.getString(check.getName() + ".flow")
                    , job
                    , endtime
                    , conf.getString(check.getName() + ".sender")));
        }
        StringBuilder sb = new StringBuilder();
        sb.append("JobEndChecker initialized ").append(this.size()).append(" checkEvents : [");
        for (JobEndCheckEvent ce : this) {
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
            LOG.debug(String.format("JobEndChecker regularly check, time range [%s, %s]", lastCheckTime, currentCheckTime));
            for (JobEndCheckEvent ce : this) {
                if (ce.shouldCheck(lastCheckTime, currentCheckTime)) {
                    try {
                        boolean ended = AzkabanMetaUtil.hasJobEnded(bds.getConnection(), ce.getProject(), ce.getFlow(), ce.getJob());
                        LOG.debug(String.format("JobEndChecker run time range [%s, %s], event %s, run=%b",
                                lastCheckTime, currentCheckTime, ce.toString(), ended));
                        if (!ended) {
                            SenderEvent se = new SenderEvent();
                            se.setType(BaseEvent.Type.JOBEND);
                            se.setProject(ce.getProject())
                                    .setFlow(ce.getFlow())
                                    .setJob(ce.getJob());
                            se.setMsg(String.format("Job not end in time, project=%s, flow=%s, job=%s, expectEnd=%s."
                                    , se.getProject(), se.getFlow(), se.getJob(), ce.getEndTimeStr()));
                            while (!queue.offer(se, interval, TimeUnit.MILLISECONDS)) {
                                LOG.warn(String.format("SenderEvent offer to dispacher failed, retry... %s", se));
                            }
                            LOG.info(String.format("Offer event to dispacher succ: %s", se));
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("JobEndChecker interrupted.", e);
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
                LOG.warn("JobStopChecker interrupted.", e);
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("JobEndChecker stopped.");
    }

    protected class JobEndCheckEvent extends BaseEvent implements ShouldCheckable {
        private long tzOffset;
        private long dayDelta;

        private String getEndTimeStr() {
            return endTimeStr;
        }

        private String endTimeStr;

        private JobEndCheckEvent(String project, String flow, String job, String endTime, String sender) throws Exception {
            setProject(project);
            setFlow(flow);
            setJob(job);
            this.setEndTime(endTime);
            setSender(sender);
            dayDelta = 24 * 60 * 60 * 1000;
        }

        @Override
        public boolean shouldCheck(long start, long end) {
            return (start + tzOffset) % dayDelta <= getEndTime() && getEndTime() < (end + tzOffset) % dayDelta;
        }

        private void setEndTime(String endTime) throws Exception {
            tzOffset = Calendar.getInstance().getTimeZone().getRawOffset();
            endTimeStr = endTime;
            super.setEndTime(new SimpleDateFormat("HH:mm").parse(endTime).getTime() + tzOffset);
        }

        @Override
        public String toString() {
            return String.format("CheckEvent: project=%s, flow=%s, job=%s, expectEnd=%s, sender=%s"
                    , getProject(), getFlow(), getJob(), endTimeStr, getSender());
        }
    }
}
