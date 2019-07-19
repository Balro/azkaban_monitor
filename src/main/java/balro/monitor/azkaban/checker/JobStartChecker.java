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

public class JobStartChecker extends ArrayList<JobStartChecker.JobStartCheckEvent> implements Runnable {
    private static Logger LOG = Logger.getLogger(JobStartChecker.class);

    private LinkedBlockingQueue<SenderEvent> queue;
    private BasicDataSource bds;

    private int interval;

    public static void setLogger(Logger logger) {
        LOG = logger;
    }

    public JobStartChecker(HierarchicalConfiguration conf, LinkedBlockingQueue<SenderEvent> queue, BasicDataSource bds, int interval) throws Exception {
        this.queue = queue;
        this.bds = bds;
        this.interval = interval;
        initCheckList(conf);
    }

    private void initCheckList(HierarchicalConfiguration conf) throws Exception {
        conf.getList("");
        ConfigurationNode cNode = conf.getRootNode();
        for (ConfigurationNode check : cNode.getChildren()) {
            String starttime = conf.getString(check.getName() + ".starttime");
            String job = conf.getString(check.getName() + ".job");
            if (job == null || starttime == null) continue;
            this.add(new JobStartCheckEvent(conf.getString(check.getName() + ".project")
                    , conf.getString(check.getName() + ".flow")
                    , job
                    , starttime
                    , conf.getString(check.getName() + ".sender")));
        }
        StringBuilder sb = new StringBuilder();
        sb.append("JobStartChecker initialized ").append(this.size()).append(" checkEvents : [");
        for (JobStartCheckEvent ce : this) {
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
            LOG.debug(String.format("JobStartChecker regularly check, time range [%s, %s]", lastCheckTime, currentCheckTime));
            for (JobStartCheckEvent ce : this) {
                if (ce.shouldCheck(lastCheckTime, currentCheckTime)) {
                    try {
                        boolean started = AzkabanMetaUtil.hasJobStarted(bds.getConnection(), ce.getProject(), ce.getFlow(), ce.getJob());
                        LOG.debug(String.format("JobStartChecker run time range [%s, %s], event %s, run=%b",
                                lastCheckTime, currentCheckTime, ce.toString(), started));
                        if (!started) {
                            SenderEvent se = new SenderEvent();
                            se.setType(BaseEvent.Type.JOBSTART);
                            se.setProject(ce.getProject())
                                    .setFlow(ce.getFlow())
                                    .setJob(ce.getJob());
                            se.setMsg(String.format("Job not start in time, project=%s, flow=%s, job=%s, expectStart=%s."
                                    , se.getProject(), se.getFlow(), se.getJob(), ce.getStartTimeStr()));
                            while (!queue.offer(se, interval, TimeUnit.MILLISECONDS)) {
                                LOG.warn(String.format("SenderEvent offer to dispacher failed, retry... %s", se));
                            }
                            LOG.info(String.format("Offer event to dispacher succ: %s", se));
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("JobStartChecker interrupted.", e);
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
                LOG.warn("JobStartChecker interrupted.", e);
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("JobStartChecker stopped.");
    }

    protected class JobStartCheckEvent extends BaseEvent implements ShouldCheckable {
        private long tzOffset;
        private long dayDelta;

        private String getStartTimeStr() {
            return startTimeStr;
        }

        private String startTimeStr;

        private JobStartCheckEvent(String project, String flow, String job, String startTime, String sender) throws Exception {
            setProject(project);
            setFlow(flow);
            setJob(job);
            this.setStartTime(startTime);
            setSender(sender);
            dayDelta = 24 * 60 * 60 * 1000;
        }

        @Override
        public boolean shouldCheck(long start, long end) {
            return (start + tzOffset) % dayDelta <= getStartTime() && getStartTime() < (end + tzOffset) % dayDelta;
        }

        private void setStartTime(String startTime) throws Exception {
            tzOffset = Calendar.getInstance().getTimeZone().getRawOffset();
            startTimeStr = startTime;
            super.setStartTime(new SimpleDateFormat("HH:mm").parse(startTime).getTime() + tzOffset);
        }

        @Override
        public String toString() {
            return String.format("CheckEvent: project=%s, flow=%s, job=%s, expectStart=%s, sender=%s"
                    , getProject(), getFlow(), getJob(), startTimeStr, getSender());
        }
    }
}
