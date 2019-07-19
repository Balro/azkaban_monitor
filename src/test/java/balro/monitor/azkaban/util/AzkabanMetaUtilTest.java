package balro.monitor.azkaban.util;

import balro.monitor.azkaban.sender.SenderEvent;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;

public class AzkabanMetaUtilTest {
    private static Connection CONN;

    static {
        AzkabanMetaUtil.setLog(Logger.getLogger("test"));
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void initConn() throws Exception {
        CONN = DriverManager.getConnection("jdbc:mysql://test03:3306/azkaban", "root", "root123");
    }

    @After
    public void closeConn() throws Exception {
        if (CONN != null && !CONN.isClosed())
            CONN.close();
    }

    @Test
    public void checkJobStatusTest() throws Exception {
        List<SenderEvent> list = AzkabanMetaUtil.checkJobStatus(CONN, 1563455278389L, 1563455308421L, 0, "70,80"
                , "%", "%", "%");
//        List<SenderEvent> list = AzkabanMetaUtil.checkJobStatus(CONN, 0L, Long.MAX_VALUE, 0, "50"
//                , "test", "%hdw_user_product_order%", "%");
        System.out.println(list.size());
    }

    @Test
    public void hasJobStartedTest() throws Exception {
//        System.out.println(AzkabanMetaUtil.hasJobStarted(CONN, "test", "ttt", "321"));
        System.out.println(AzkabanMetaUtil.hasJobStarted(CONN, "test", "ttt", "123"));
    }

    @Test
    public void hasJobEndedTest() throws Exception {
//        System.out.println(AzkabanMetaUtil.hasJobEnded(CONN, "test", "ttt", "321"));
        System.out.println(AzkabanMetaUtil.hasJobEnded(CONN, "test", "ttt", "123"));
    }

    @Test
    public void hasFlowStartedTest() throws Exception {
        System.out.println(AzkabanMetaUtil.hasFlowStarted(CONN, "test", "end"));
    }

    @Test
    public void hasFlowEndedTest() throws Exception {
        System.out.println(AzkabanMetaUtil.hasFlowEnded(CONN, "test", "end"));
    }

}
