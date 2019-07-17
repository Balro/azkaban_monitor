package baluo.monitor.azkaban;

import baluo.monitor.azkaban.checker.CheckerEnum;
import baluo.monitor.azkaban.checker.JobStatusChecker;
import baluo.monitor.azkaban.conf.ConfConst;
import baluo.monitor.azkaban.dispatcher.Dispacher;
import baluo.monitor.azkaban.heartbeater.HeartBeater;
import baluo.monitor.azkaban.sender.OnealertSender;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.dbcp.BasicDataSource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AzkabanMonitor {
    private static XMLConfiguration conf;
    private static HeartBeater hb;
    private static ExecutorService checkers = Executors.newCachedThreadPool();
    private static ExecutorService senders = Executors.newCachedThreadPool();
    private static Dispacher disp;
    private static BasicDataSource bds;

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "start":
                start();
                break;
            case "stop":
                stop();
                break;
            case "status":
                status();
                break;
            default:
                System.err.println("Unknown command " + args[0]);
        }
    }

    private static void start() throws Exception {
        conf = new XMLConfiguration("azkaban-monitor.xml");
        startDispacher();
        startSender();
        startChecker();
        startHeartBeat();
    }

    private static void startChecker() throws Exception {
        initBDS();
        for (CheckerEnum checkerName : CheckerEnum.values()) {
            Runnable checker = (Runnable) Class.forName(checkerName.name()).getConstructor(XMLConfiguration.class, Dispacher.class, BasicDataSource.class)
                    .newInstance(conf.configurationAt(ConfConst.CONDITIONS), disp, bds);
            checkers.execute(checker);
        }
    }

    private static void initBDS() {
        bds.setDriverClassName(conf.getString(ConfConst.DATABASE_DRIVER, ConfConst.DATABASE_DRIVER_DEF));
        bds.setUrl(conf.getString(ConfConst.DATABASE_URL));
        bds.setUsername(conf.getString(ConfConst.DATABASE_USER));
        bds.setPassword(conf.getString(ConfConst.DATABASE_PASSWORD));
        bds.setInitialSize(1);
        bds.setMaxActive(10);
        bds.setMinIdle(1);
        bds.setMaxIdle(10);
        bds.setMinEvictableIdleTimeMillis(1000 * 60 * 30);
    }

    private static void startSender() {
        HierarchicalConfiguration senderConf = conf.configurationAt(ConfConst.SENDERS);
        ConfigurationNode node = senderConf.getRootNode();
        for (ConfigurationNode child : node.getChildren()) {
            OnealertSender sender = new OnealertSender((String) child.getChildren("app").get(0).getValue()
                    , (String) child.getChildren("week").get(0).getValue()
                    , (String) child.getChildren("hour").get(0).getValue());
            disp.addSender(sender);
            senders.submit(sender);
        }
    }

    private static void startDispacher() {
        disp = new Dispacher(conf);
        new Thread(disp).start();
    }

    private static void startHeartBeat() throws Exception {
        hb = new HeartBeater(conf.getString(ConfConst.ZOO_QUORUM, ConfConst.ZOO_QUORUM_DEF)
                , conf.getInt(ConfConst.ZOO_TIMEOUT_MILLI, ConfConst.ZOO_TIMEOUT_MILLI_DEF)
                , conf.getString(ConfConst.ZOO_NODE, ConfConst.ZOO_NODE_DEF)
                , conf.getInt(ConfConst.MONITOR_HEART_BEAT_INTERVAL_MILLI, ConfConst.MONITOR_HEART_BEAT_INTERVAL_MILLI_DEF));
        Thread hb = new Thread(AzkabanMonitor.hb);
        hb.setDaemon(true);
        hb.start();
    }

    private static void stop() {
        checkers.shutdown();
        disp.stop();
        senders.shutdown();
    }

    private static void status() {
        System.out.println(hb.status());
    }
}
