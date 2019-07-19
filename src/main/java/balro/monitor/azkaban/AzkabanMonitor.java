package balro.monitor.azkaban;

import balro.monitor.azkaban.checker.CheckerEnum;
import balro.monitor.azkaban.heartbeater.HeartBeater;
import balro.monitor.azkaban.conf.ConfConst;
import balro.monitor.azkaban.dispatcher.Dispacher;
import balro.monitor.azkaban.sender.OnealertSender;
import balro.monitor.azkaban.util.ZKUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class AzkabanMonitor {
    private static final Logger LOG = Logger.getLogger(AzkabanMonitor.class);
    private static XMLConfiguration conf;
    private static HeartBeater hb;
    private static ExecutorService checkers = Executors.newCachedThreadPool();
    private static ExecutorService senders = Executors.newCachedThreadPool();
    private static Dispacher disp;
    private static BasicDataSource bds = new BasicDataSource();

    public static void main(String[] args) {
        try {
            switch (args[0]) {
                case "start":
                    start();
                    break;
                case "status":
                    status();
                    break;
                default:
                    System.err.println("Unknown command " + args[0]);
            }
        } catch (IndexOutOfBoundsException iob) {
            stop();
        }
    }

    private static void start() {
        LOG.info("Azkaban monitor starting.");
        try {
            loadConf();
            startDispacher();
            startSender();
            startChecker();
            startHeartBeat();
            LOG.info("Azkaban monitor started.");
        } catch (Exception e) {
            LOG.error("Azkaban monitor start failed.", e);
            stop();
        }
    }

    private static void loadConf() throws Exception {
        conf = new XMLConfiguration();
        conf.setDelimiterParsingDisabled(true);
        try {
            conf.load("azkaban-monitor.xml");
        } catch (ConfigurationException ce) {
            LOG.warn(null, ce);
            conf.load("/etc/azkaban-monitor/azkaban-monitor.xml");
        }
    }

    private static void startChecker() throws Exception {
        LOG.info("Starting checkers.");
        initBDS();
        String pack = "balro.monitor.azkaban.checker";
        for (CheckerEnum checkerName : CheckerEnum.values()) {
            Runnable checker = (Runnable) Class.forName(pack + "." + checkerName.name()).getConstructor(HierarchicalConfiguration.class, LinkedBlockingQueue.class, BasicDataSource.class, int.class)
                    .newInstance(conf.configurationAt(ConfConst.CONDITIONS), disp.getDisp(), bds, conf.getInt(ConfConst.MONITOR_HEART_BEAT_INTERVAL_MILLI, ConfConst.MONITOR_HEART_BEAT_INTERVAL_MILLI_DEF));
            checkers.execute(checker);
            LOG.info(String.format("Checker %s initialized.", checkerName));
        }
        LOG.info("Checkers started.");
    }

    private static void initBDS() {
        LOG.info("Initialize BDS.");
        bds.setDriverClassName(conf.getString(ConfConst.DATABASE_DRIVER, ConfConst.DATABASE_DRIVER_DEF));
        bds.setUrl(conf.getString(ConfConst.DATABASE_URL));
        bds.setUsername(conf.getString(ConfConst.DATABASE_USER));
        bds.setPassword(conf.getString(ConfConst.DATABASE_PASSWORD));
        bds.setInitialSize(1);
        bds.setMaxActive(10);
        bds.setMinIdle(1);
        bds.setMaxIdle(10);
        bds.setMinEvictableIdleTimeMillis(1000 * 60 * 30);
        LOG.info("BDS initialized.");
    }

    private static void startSender() {
        LOG.info("Starting senders.");
        HierarchicalConfiguration senderConf = conf.configurationAt(ConfConst.SENDERS);
        ConfigurationNode node = senderConf.getRootNode();
        for (ConfigurationNode child : node.getChildren()) {
            OnealertSender sender = new OnealertSender(child.getName()
                    , (String) child.getChildren("app").get(0).getValue()
                    , (int) conf.getLong(ConfConst.MONITOR_HEART_BEAT_INTERVAL_MILLI, ConfConst.MONITOR_HEART_BEAT_INTERVAL_MILLI_DEF)
                    , (String) child.getChildren("week").get(0).getValue()
                    , (String) child.getChildren("hour").get(0).getValue());
            disp.addSender(sender);
            senders.submit(sender);
        }
        LOG.info("Senders started.");
    }

    private static void startDispacher() {
        LOG.info("Starting dispacher.");
        disp = new Dispacher(conf);
        disp.start();
        LOG.info("Dispacher started");
    }

    private static void startHeartBeat() throws Exception {
        LOG.info("Starting heartBeater.");
        hb = new HeartBeater(conf.getString(ConfConst.ZOO_QUORUM, ConfConst.ZOO_QUORUM_DEF)
                , conf.getInt(ConfConst.ZOO_TIMEOUT_MILLI, ConfConst.ZOO_TIMEOUT_MILLI_DEF)
                , conf.getString(ConfConst.ZOO_NODE, ConfConst.ZOO_NODE_DEF)
                , conf.getInt(ConfConst.MONITOR_HEART_BEAT_INTERVAL_MILLI, ConfConst.MONITOR_HEART_BEAT_INTERVAL_MILLI_DEF));
        Thread hb = new Thread(AzkabanMonitor.hb);
        hb.setDaemon(true);
        hb.start();
        LOG.info("HeartBeater started.");
    }

    public static void stop() {
        LOG.info("Azkaban monitor stopping.");
        try {
            disp.interrupt();
            senders.shutdown();
            checkers.shutdownNow();
            bds.close();
            if (!senders.awaitTermination(5, TimeUnit.SECONDS)) {
                senders.shutdownNow();
            }
        } catch (Exception e) {
            LOG.warn(null, e);
        }
        LOG.info("Azkaban monitor stopped.");
    }

    private static void status() {
        try {
            loadConf();
            HeartBeater.status(conf.getString(ConfConst.ZOO_QUORUM, ConfConst.ZOO_QUORUM_DEF)
                    , 5000
                    , conf.getString(ConfConst.ZOO_NODE, ConfConst.ZOO_NODE_DEF));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
