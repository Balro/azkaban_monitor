package monitor.azkaban.util;

import monitor.azkaban.sender.SenderEvent;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class AzkabanMetaUtil {
    private static Logger LOG = Logger.getLogger(AzkabanMetaUtil.class);
    private static BasicDataSource bds;
    private static long tzOffset = Calendar.getInstance().getTimeZone().getRawOffset();

    public static void setLog(Logger log) {
        LOG = log;
    }

    /**
     * @param status "(50,60,70)"
     * @return 1 exec_id,
     * 2 project,
     * 3 flow_id,
     * 4 job_id,
     * 5 attempt,
     * 6 start_time,
     * 7 end_time
     */
    public static List<SenderEvent> checkJobStatus(Connection conn, long timeStart, long timeEnd, int attempt
            , String status, String project, String flow, String job) throws Exception {
        String sql = String.format("select exec_id,name,flow_id,job_id,attempt,start_time,end_time,status " +
                "from projects join execution_jobs on project_id = id " +
                "where end_time between ? and ? and attempt >= ? and status in (%s) " +
                "and name like ? and flow_id like ? and job_id like ?", status);
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setLong(1, timeStart);
        ps.setLong(2, timeEnd);
        ps.setInt(3, attempt);
        ps.setString(4, project);
        ps.setString(5, flow);
        ps.setString(6, job);
        LOG.debug("CheckJobStatus sql: " + ps.toString().substring(ps.toString().lastIndexOf(":") + 1).trim());

        ResultSet rs = ps.executeQuery();
        List<SenderEvent> seList = new ArrayList<>();
        while (rs.next()) {
            seList.add((SenderEvent) new SenderEvent().setExecId(rs.getLong(1))
                    .setProject(rs.getString(2))
                    .setFlow(rs.getString(3))
                    .setJob(rs.getString(4))
                    .setAttempt(rs.getInt(5))
                    .setStartTime(rs.getLong(6))
                    .setEndTime(rs.getLong(7))
                    .setStatus(rs.getString(8)));
        }
        rs.close();
        ps.close();
        conn.close();
        return seList;
    }

    public static boolean hasJobStarted(Connection conn, String project, String flow, String job) throws Exception {
        String sql = "select 1 " +
                "from projects join execution_jobs on project_id = id " +
                "where start_time >= ?  " +
                "and name like ? and flow_id like ? and job_id like ? " +
                "limit 1";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setLong(1, midnight());
        ps.setString(2, project);
        ps.setString(3, flow);
        ps.setString(4, job);
        LOG.debug("JobStartCheck sql: " + ps.toString().substring(ps.toString().lastIndexOf(":") + 1).trim());
        return haveData(ps, conn);
    }

    public static boolean hasJobEnded(Connection conn, String project, String flow, String job) throws Exception {
        String sql = "select 1 " +
                "from projects join execution_jobs on project_id = id " +
                "where end_time >= ?  " +
                "and name like ? and flow_id like ? and job_id like ? " +
                "limit 1";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setLong(1, midnight());
        ps.setString(2, project);
        ps.setString(3, flow);
        ps.setString(4, job);
        LOG.debug("JobEndCheck sql: " + ps.toString().substring(ps.toString().lastIndexOf(":") + 1).trim());
        return haveData(ps, conn);
    }

    public static boolean hasFlowStarted(Connection conn, String project, String flow) throws Exception {
        String sql = "select 1 " +
                "from projects join execution_flows on project_id = id " +
                "where start_time >= ?  " +
                "and name like ? and flow_id like ? " +
                "limit 1";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setLong(1, midnight());
        ps.setString(2, project);
        ps.setString(3, flow);
        LOG.debug("FlowStartCheck sql: " + ps.toString().substring(ps.toString().lastIndexOf(":") + 1).trim());
        return haveData(ps, conn);
    }

    public static boolean hasFlowEnded(Connection conn, String project, String flow) throws Exception {
        String sql = "select 1 " +
                "from projects join execution_flows on project_id = id " +
                "where end_time >= ?  " +
                "and name like ? and flow_id like ? " +
                "limit 1";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setLong(1, midnight());
        ps.setString(2, project);
        ps.setString(3, flow);
        LOG.debug("FlowEndCheck sql: " + ps.toString().substring(ps.toString().lastIndexOf(":") + 1).trim());
        return haveData(ps, conn);
    }

    private static boolean haveData(PreparedStatement ps, Connection conn) throws Exception {
        boolean has = ps.executeQuery().next();
        ps.close();
        conn.close();
        return has;
    }

    private static long midnight() {
        long s = System.currentTimeMillis();
        return s - (s + tzOffset) % 86400000L;
    }
}
