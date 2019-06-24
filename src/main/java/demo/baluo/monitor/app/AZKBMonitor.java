package demo.baluo.monitor.app;

import demo.baluo.monitor.heartbeat.HeartBeater;
import demo.baluo.monitor.jdbc.JDBCUtil;
import demo.baluo.monitor.sender.SendEvent;
import demo.baluo.monitor.sender.Sender;
import demo.baluo.monitor.sender.SenderOnealert;
import demo.baluo.monitor.zookeeper.ZKUtil;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class AZKBMonitor {
    private static final Logger LOG = Logger.getLogger(AZKBMonitor.class);
    private static Properties prop;
    private ZKUtil zk;
    private JDBCUtil jdbc;

    public static void main(String[] args) throws Exception {
        AZKBMonitor app = new AZKBMonitor();
        app.init();
        app.start();
    }

    private void start() throws Exception {
        new JobChecker().init().start();
        new HeartBeater(zk, prop).init().start();
    }

    private void init() throws Exception {
        prop = new Properties();
        prop.load(this.getClass().getClassLoader().getResourceAsStream("monitor.properties"));
        zk = new ZKUtil(prop).init("azkb");
        jdbc = new JDBCUtil(prop).init("azkb");
        LOG.info("AZKBMonitor initiallized.");
    }

    private class JobChecker extends Thread {
        private CheckTool ct = new CheckTool();
        private List<CheckEvent> ceList = new ArrayList<>();
        private List<Sender> sdList = new ArrayList<>();
        private long lastCheckTime = System.currentTimeMillis();

        private JobChecker init() {
            initCEList();
            initSDList();
            return this;
        }

        private void initCEList() {
            String[] ces = prop.getProperty("azkb.event.list").split(",");
            for (String ce : ces) {
                String type = prop.getProperty("azkb.event." + ce + ".type", "null");
                switch (type) {
                    case "allfail":
                        ceList.add(new CheckEvent().setType("allfail"));
                        break;
                    case "flow":
                        String flowNmae = prop.getProperty("azkb.event." + ce + ".flowName");
                        if (flowNmae == null) {
                            LOG.fatal("CheckEvent init failed: " + ce + ", flowName is null");
                            System.exit(1);
                        }
                        ceList.add(new CheckEvent().setType("flow")
                                .setFlowName(prop.getProperty("azkb.event." + ce + ".flowName"))
                                .setStartTime(Integer.parseInt(prop.getProperty("azkb.event." + ce + ".startTime", "-1")))
                                .setEndTime(Integer.parseInt(prop.getProperty("azkb.event." + ce + ".endTime", "-1"))));
                        break;
                    case "job":
                        String jobName = prop.getProperty("azkb.event." + ce + ".jobName");
                        if (jobName == null) {
                            LOG.fatal("CheckEvent init failed: " + ce + ", jobName is null");
                            System.exit(1);
                        }
                        ceList.add(new CheckEvent().setType("job")
                                .setJobName(prop.getProperty("azkb.event." + ce + ".jobName"))
                                .setStartTime(Integer.parseInt(prop.getProperty("azkb.event." + ce + ".startTime", "-1")))
                                .setEndTime(Integer.parseInt(prop.getProperty("azkb.event." + ce + ".endTime", "-1"))));
                        break;
                    default:
                        LOG.error("CheckEvent init failed: " + ce + " 's type [" + type + "] is not valid.");
                        System.exit(1);
                }
            }
            StringBuilder sb = new StringBuilder();
            for (CheckEvent ce : ceList) {
                sb.append("[");
                sb.append(ce);
                sb.append("],");
            }
            LOG.info("CheckEventList initialized: " + sb.toString());
        }

        private void initSDList() {
            String[] sds = prop.getProperty("azkb.sender.list").split(",");
            for (String sd : sds) {
                switch (prop.getProperty("azkb.sender." + sd + ".type", "")) {
                    case "onealert":
                        Sender sender = new SenderOnealert(prop.getProperty("azkb.sender." + sd + ".app"));
                        sender.init(sd, prop.getProperty("azkb.sender." + sd + ".workTime"));
                        sdList.add(sender);
                        break;
                    default:
                        LOG.warn("Sender imp [" + sd + "] not found.");
                        break;
                }
            }
            for (Sender sd : sdList) {
                StringBuilder sb = new StringBuilder();
                sb.append("SenderList initialized: ");
                LOG.info(sb);
                sb.append(sd);
            }
        }

        @Override
        public void run() {
            while (!this.isInterrupted()) {
                long tmp = System.currentTimeMillis();
                for (CheckEvent ce : ceList) {
                    try {
                        List<SendEvent> tmpSE = process(ce);
                        if (tmpSE == null || tmpSE.size() == 0)
                            continue;
                        for (Sender sd : sdList) {
                            if (sd.checkActive())
                                sd.send(process(ce));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                lastCheckTime = tmp;
                try {
                    sleep(Long.parseLong(prop.getProperty("azkb.checkInterval.milliseconds")));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    this.interrupt();
                }
            }
        }

        private List<SendEvent> process(CheckEvent ce) throws Exception {
            LOG.info("process checkevent: " + ce);
            switch (ce.type) {
                case "allfail": {
                    return ct.getFailedJobs(lastCheckTime);
                }
                case "flow": {
                    List<SendEvent> flowEvent;
                    if (ce.startTime >= ct.getTime(lastCheckTime) && ce.startTime < ct.getTime(System.currentTimeMillis())) {
                        // include status 30/70
                        LOG.debug("check flow :" + ce.flowName + ",expect_start_time:" + ce.startTime);
                        flowEvent = ct.hasFlowStarted(ce.flowName, ce.startTime);
                        if (flowEvent != null && flowEvent.size() > 0) {
                            return flowEvent;
                        }
                    }
                    if (ce.endTime >= ct.getTime(lastCheckTime) && ce.endTime < ct.getTime(System.currentTimeMillis())) {
                        LOG.debug("check flow :" + ce.flowName + ",expect_end_time:" + ce.endTime);
                        // only check the status equals 50.
                        return ct.hasFlowEnded(ce.flowName, ce.endTime);
                    }
                    break;
                }
                case "job": {
                    List<SendEvent> jobEvent = null;
                    if (ce.startTime > 0) {
                        if (ce.startTime >= ct.getTime(lastCheckTime) && ce.startTime < ct.getTime(System.currentTimeMillis())) {
                            LOG.debug("check job :" + ce.jobName + ",expect_end_time:" + ce.startTime);
                            // include status 30/70
                            jobEvent = ct.hasJobStarted(ce.jobName, ce.startTime);
                        }
                        if (jobEvent != null && jobEvent.size() > 0) {
                            LOG.debug("check job :" + ce.jobName + ",expect_end_time:" + ce.endTime);
                            return jobEvent;
                        }
                    }
                    if (ce.endTime >= ct.getTime(lastCheckTime) && ce.endTime < ct.getTime(System.currentTimeMillis())) {
                        // only check the status equals 50.
                        return ct.hasJobEnded(ce.jobName, ce.endTime);
                    }
                    break;
                }
                default:
                    LOG.warn("CheckEvent type [" + ce.type + "] not correct.");
            }
            return null;
        }

    }

    private class CheckTool {
        private Calendar cal = Calendar.getInstance();
        private DateFormat dfLog = new SimpleDateFormat("(y-M-d HH:mm:ss)");
        private DateFormat dfTime = new SimpleDateFormat("HHmm");

        private int getTime(Long timeStamp) {
            return Integer.parseInt(dfTime.format(new Date(timeStamp)));
        }

        private long midnightTimeStamp() {
            cal.setTimeInMillis(System.currentTimeMillis());
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            return cal.getTimeInMillis();
        }

        /**
         * select count(*) from execution_flows where start_time > 1555576698384 and flow_id = 'start' and status in (30,50);
         */
        private List<SendEvent> hasFlowStarted(String flow, int expStartTime) throws Exception {
            LOG.debug("check has flow started: " + flow);
            List<SendEvent> ses = new LinkedList<>();
            PreparedStatement ps = jdbc.getConn().prepareStatement(
                    "select count(*) from execution_flows " +
                            "where start_time > ? and flow_id = ? and status in (30,50)");
            ps.setLong(1, midnightTimeStamp());
            ps.setString(2, flow);
            ResultSet rs = ps.executeQuery();
            if (rs.next() && rs.getInt(1) == 0) {
                SendEvent se = new SendEvent();
                se.id = "flow_not_start/" + flow + "/" + expStartTime;
                se.title = "Azkaban作业未开始执行, flowName: " + flow + ", 期望开始时间: " + expStartTime;
                se.msg = "Azkaban作业未开始执行, flowName: " + flow + ", 期望开始时间: " + expStartTime;
                ses.add(se);
            }
            return ses;
        }

        /**
         * select count(*) from execution_flows where end_time > 1555576698384 and flow_id = 'start' and status = 50;
         */
        private List<SendEvent> hasFlowEnded(String flow, int expEndTime) throws Exception {
            LOG.debug("check has flow ended: " + flow);
            List<SendEvent> ses = new LinkedList<>();
            PreparedStatement ps = jdbc.getConn().prepareStatement(
                    "select count(*) from execution_flows " +
                            "where end_time > ? and flow_id = ? and status in (50,60,70)");
            ps.setLong(1, midnightTimeStamp());
            ps.setString(2, flow);
            ResultSet rs = ps.executeQuery();
            if (rs.next() && rs.getInt(1) == 0) {
                SendEvent se = new SendEvent();
                se.id = "flow_not_end/" + flow + "/" + expEndTime;
                se.title = "Azkaban作业未正常结束, flowName: " + flow + ", 期望结束时间: " + expEndTime;
                se.msg = "Azkaban作业未正常结束, flowName: " + flow + ", 期望结束时间: " + expEndTime;
                ses.add(se);
            }
            return ses;
        }

        /**
         * select count(*) from execution_jobs where start_time > 1555576698384 and job_id = 'start' and status = 50;
         */
        private List<SendEvent> hasJobStarted(String job, int expStartTime) throws Exception {
            LOG.debug("check has job started: " + job);
            List<SendEvent> ses = new LinkedList<>();
            PreparedStatement ps = jdbc.getConn().prepareStatement(
                    "select count(*) from execution_jobs " +
                            "where start_time > ? and job_id = ? and status in (30,50)");
            ps.setLong(1, midnightTimeStamp());
            ps.setString(2, job);
            ResultSet rs = ps.executeQuery();
            if (rs.next() && rs.getInt(1) == 0) {
                SendEvent se = new SendEvent();
                se.id = "job_not_start/" + job + "/" + expStartTime;
                se.title = "Azkaban作业未开始执行, jobName: " + job + ", 期望开始时间: " + expStartTime;
                se.msg = "Azkaban作业未开始执行, jobName: " + job + ", 期望开始时间: " + expStartTime;
                ses.add(se);
            }
            return ses;
        }

        /**
         * select count(*) from execution_jobs where end_time > 1555576698384 and job_id = 'start' and status = 50;
         */
        private List<SendEvent> hasJobEnded(String job, int expEndTime) throws Exception {
            LOG.debug("check has job ended: " + job);
            List<SendEvent> ses = new LinkedList<>();
            PreparedStatement ps = jdbc.getConn().prepareStatement(
                    "select count(*) from execution_jobs " +
                            "where end_time > ? and job_id = ? and status in (50,60,70)");
            ps.setLong(1, midnightTimeStamp());
            ps.setString(2, job);
            ResultSet rs = ps.executeQuery();
            if (rs.next() && rs.getInt(1) == 0) {
                SendEvent se = new SendEvent();
                se.id = "job_not_end/" + job + "/" + expEndTime;
                se.title = "Azkaban作业未正常结束, jobName: " + job + ", 期望结束时间: " + expEndTime;
                se.msg = "Azkaban作业未正常结束, jobName: " + job + ", 期望结束时间: " + expEndTime;
                ses.add(se);
            }
            return ses;
        }

        /**
         * select exec_id,flow_id,job_id,attempt,start_time,end_time,status from execution_jobs where end_time > lastCheckTime and attempt > 0 and status in (60,70);
         */
        private List<SendEvent> getFailedJobs(long lastCheckTime) throws Exception {
            LOG.debug("check failed jobs from: " + lastCheckTime);
            List<SendEvent> ses = new LinkedList<>();
            PreparedStatement ps = jdbc.getConn().prepareStatement(
                    "select exec_id,flow_id,job_id,attempt,start_time,end_time,status from execution_jobs " +
                            "where end_time > ? and attempt > 0 and status in (60,70)");
            ps.setLong(1, lastCheckTime);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String exec_id = rs.getString(1);
                String flow_id = rs.getString(2);
                String job_id = rs.getString(3);
                String attempt = rs.getString(4);
                String start_time = rs.getString(5);
                String end_time = rs.getString(6);
                String status = rs.getString(7);
                LOG.info("Get failed job: exec_id: " + exec_id + ", flow_id: " + flow_id + ", job_id: " + job_id +
                        ", attemp: " + attempt + ", start_time: " + start_time + ", end_time: " + end_time + ", status: " + status);
                SendEvent se = new SendEvent();
                se.id = exec_id + "/" + flow_id + "/" + job_id + "/" + end_time + "/" + status;
                se.title = "Azkaban作业失败!exec_id:" + exec_id + "/flow:" + flow_id + "/job:" + job_id + "/attempt:" + attempt + "/end_time:" + end_time;
                se.msg = "Azkaban作业失败!exec_id:" + exec_id + "/flow:" + flow_id + "/job:" + job_id + "/attempt:" + attempt + "/end_time:" + dfLog.format(new Date(Long.parseLong(end_time)));
                ses.add(se);
            }
            return ses;
        }

    }

    private class CheckEvent {
        // type: flow , job , allfail .
        private String type;
        private String flowName;
        private String jobName;
        private int startTime = -1;
        private int endTime = -1;

        private CheckEvent setType(String type) {
            this.type = type;
            return this;
        }

        private CheckEvent setFlowName(String flowName) {
            this.flowName = flowName;
            return this;
        }

        private CheckEvent setJobName(String jobName) {
            this.jobName = jobName;
            return this;
        }

        private CheckEvent setStartTime(int startTime) {
            this.startTime = startTime;
            return this;
        }

        private CheckEvent setEndTime(int endTime) {
            this.endTime = endTime;
            return this;
        }

        @Override
        public String toString() {
            return "type:" +
                    type +
                    "/flowName:" +
                    flowName +
                    "/jobName:" +
                    jobName +
                    "/startTime:" +
                    startTime +
                    "/endTime:" +
                    endTime;
        }
    }
}
