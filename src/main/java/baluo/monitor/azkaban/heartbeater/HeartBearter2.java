package baluo.monitor.azkaban.heartbeater;

import baluo.monitor.azkaban.AzkabanMonitor2;
import baluo.monitor.azkaban.util.ZKUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.net.InetAddress;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class HeartBearter2 implements Runnable {
    private static Logger LOG = Logger.getLogger(HeartBearter2.class);
    private final String node;
    private final int interval;
    private final ZooKeeper zoo;
    private final String hostname;
    private final String ip;

    public static void setLOG(Logger log) {
        LOG = log;
    }

    public HeartBearter2(String quorum, int timeout, String node, int interval) throws Exception {
        this.node = node;
        this.interval = interval;
        hostname = InetAddress.getLocalHost().getHostName();
        ip = InetAddress.getLocalHost().getHostAddress();
        zoo = new ZooKeeper(quorum, timeout, ZKUtil.EMPTY_WATCHER);
        ZKUtil.create(zoo, node, data(), CreateMode.EPHEMERAL);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ZKUtil.delete(zoo, node);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                })
        );
    }

    @Override
    public void run() {
        try {
            while (ZKUtil.exist(zoo, node)) {
                ZKUtil.set(zoo, node, data());
                TimeUnit.SECONDS.sleep(10);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        AzkabanMonitor2.RUN = false;
        LOG.fatal("We just lost the heart, prepare to stop.");
    }

    private void initCheck() {
        AzkabanMonitor2.RUN = false;
        LOG.error("There is a AzkabanMonitor running " + status());
    }

    public String status() {
        try {
            return ZKUtil.get(zoo, node);
        } catch (Exception e) {
            LOG.warn("Get status failed.");
            e.printStackTrace();
        }
        return null;
    }

    private String data() {
        return String.format("%s:%s  %s", hostname, ip, new Date());
    }
}
