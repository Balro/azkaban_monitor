package balro.monitor.azkaban.heartbeater;

import balro.monitor.azkaban.AzkabanMonitor;
import balro.monitor.azkaban.util.ZKUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.net.InetAddress;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class HeartBeater implements Runnable {
    private static Logger LOG = Logger.getLogger(HeartBeater.class);
    private final String node;
    private final int interval;
    private final ZooKeeper zoo;
    private final String hostname;
    private final String ip;

    public static void setLOG(Logger log) {
        LOG = log;
    }

    /**
     * @param quorum   quorum.
     * @param timeout  millieseconds.
     * @param node     heartbeat node, parent node must be create manually.
     * @param interval millieseconds, update new data.
     * @throws Exception exception
     */
    public HeartBeater(String quorum, int timeout, String node, int interval) throws Exception {
        this.node = node;
        this.interval = interval;
        hostname = InetAddress.getLocalHost().getHostName();
        ip = InetAddress.getLocalHost().getHostAddress();
        zoo = new ZooKeeper(quorum, timeout, ZKUtil.EMPTY_WATCHER);
        ZKUtil.create(zoo, node, data(), CreateMode.EPHEMERAL);
        LOG.info(String.format("HeartBeat initialized, zoo=%s, timeout=%d, node=%s", quorum, timeout, node));
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ZKUtil.delete(zoo, node);
                        } catch (Exception e) {
                            LOG.warn(null, e);
                        }
                    }
                })
        );
    }

    @Override
    public void run() {
        try {
            while (ZKUtil.exist(zoo, node)) {
                String data = data();
                try {
                    ZKUtil.set(zoo, node, data);
                } catch (KeeperException ke) {
                    LOG.warn(null, ke);
                }
                LOG.debug(String.format("Boom %s.", data));
                TimeUnit.MILLISECONDS.sleep(interval);
            }
        } catch (InterruptedException ie) {
            LOG.error(null, ie);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.error(null, e);
        }
        AzkabanMonitor.stop();
        try {
            zoo.close();
        } catch (InterruptedException ie) {
            LOG.warn(null, ie);
            Thread.currentThread().interrupt();
        }
        LOG.fatal("We just lost the heart!");
    }

    public static void status(String quorum, int timeout, String node) {
        ZooKeeper zoo = null;
        try {
            zoo = new ZooKeeper(quorum, timeout, ZKUtil.EMPTY_WATCHER);
            System.out.println(ZKUtil.get(zoo, node));
        } catch (KeeperException.NoNodeException nne) {
            System.err.println("Azkaban-monitor is down.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (zoo != null)
                try {
                    zoo.close();
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                    Thread.currentThread().interrupt();
                }
        }
    }

    private String data() {
        return String.format("%s:%s  %s", hostname, ip, new Date());
    }
}
