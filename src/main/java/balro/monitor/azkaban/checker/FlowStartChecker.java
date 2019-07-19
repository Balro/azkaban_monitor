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

public class FlowStartChecker extends ArrayList<FlowStartChecker.FlowStartCheckEvent> implements Runnable {
    private static Logger LOG = Logger.getLogger(FlowStartChecker.class);

    private LinkedBlockingQueue<SenderEvent> queue;
    private BasicDataSource bds;

    private int interval;

    public static void setLogger(Logger logger) {
        LOG = logger;
    }

    public FlowStartChecker(HierarchicalConfiguration conf, LinkedBlockingQueue<SenderEvent> queue, BasicDataSource bds, int interval) throws Exception {
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
            String flow = conf.getString(check.getName() + ".flow");
            String job = conf.getString(check.getName() + ".job");
            if (job != null || flow == null || starttime == null) continue;
            this.add(new FlowStartCheckEvent(conf.getString(check.getName() + ".project")
                    , flow
                    , starttime
                    , conf.getString(check.getName() + ".sender")));
        }
        StringBuilder sb = new StringBuilder();
        sb.append("FlowStartChecker initialized ").append(this.size()).append(" checkEvents : [");
        for (FlowStartCheckEvent ce : this) {
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
            LOG.debug(String.format("FlowStartChecker regularly check, time range [%s, %s]", lastCheckTime, currentCheckTime));
            for (FlowStartCheckEvent ce : this) {
                if (ce.shouldCheck(lastCheckTime, currentCheckTime)) {
                    try {
                        boolean started = AzkabanMetaUtil.hasFlowStarted(bds.getConnection(), ce.getProject(), ce.getFlow());
                        LOG.debug(String.format("FlowStartChecker run time range [%s, %s], event %s, run=%b",
                                lastCheckTime, currentCheckTime, ce.toString(), started));
                        if (!started) {
                            SenderEvent se = new SenderEvent();
                            se.setType(BaseEvent.Type.FLOWSTART);
                            se.setProject(ce.getProject())
                                    .setFlow(ce.getFlow());
                            se.setMsg(String.format("Flow not start in time, project=%s, flow=%s, expectStart=%s."
                                    , se.getProject(), se.getFlow(), ce.getStartTimeStr()));
                            while (!queue.offer(se, interval, TimeUnit.MILLISECONDS)) {
                                LOG.warn(String.format("SenderEvent offer to dispacher failed, retry... %s", se));
                            }
                            LOG.info(String.format("Offer event to dispacher succ: %s", se));
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("FlowStartChecker interrupted.", e);
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
                LOG.warn("FlowStartChecker interrupted.", e);
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("FlowStartChecker stopped.");
    }

    protected class FlowStartCheckEvent extends BaseEvent implements ShouldCheckable {
        private long tzOffset;
        private long dayDelta;

        private String getStartTimeStr() {
            return startTimeStr;
        }

        private String startTimeStr;

        private FlowStartCheckEvent(String project, String flow, String startTime, String sender) throws Exception {
            setProject(project);
            setFlow(flow);
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
            return String.format("CheckEvent: project=%s, flow=%s, expectStart=%s, sender=%s"
                    , getProject(), getFlow(), startTimeStr, getSender());
        }
    }
}
