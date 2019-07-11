package baluo.monitor.azkaban.heartbeater;

import baluo.monitor.azkaban.zookeeper.ZKUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.util.Date;
import java.util.Properties;

/**
 * 创建临时节点，并定时向节点put数据。
 * 数据格式 hostname(ip)
 */
public class HeartBeater extends Thread {
    private static final Logger LOG = Logger.getLogger(HeartBeater.class);
    private InetAddress addr;
    private String hbPath;
    private ZKUtil zk;
    private Properties prop;
    private long hbInterval;

    public HeartBeater(ZKUtil zk, Properties prop) {
        this.zk = zk;
        this.prop = prop;
    }

    public HeartBeater init() throws Exception {
        setDaemon(true);
        hbInterval = Long.parseLong(prop.getProperty("heartbeat.interval"));
        addr = InetAddress.getLocalHost();
        hbPath = zk.getZkroot() + "/heartbeat";
        if (zk.exist(hbPath)) {
            String formerMSG = zk.getData(hbPath);
            LOG.error("heart beat node: [" + hbPath + "] exists! formerMSG: " + formerMSG + ", ctime: " + new Date(zk.getState(hbPath).getCtime()) + ".");
            LOG.fatal("System exit!");
            System.exit(1);
        }
        zk.create(hbPath, addr.getHostName() + "(" + addr.getHostAddress() + ") ", CreateMode.EPHEMERAL);
        LOG.info("HeartBeater initiallized.");
        return this;
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                zk.delete(hbPath);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        int maxRetry = Integer.parseInt(prop.getProperty("zookeeper.maxretry"));
        int hasRetry = 0;
        while (true) {
            try {
                sleep(hbInterval);
                zk.setData(hbPath, addr.getHostName() + "(" + addr.getHostAddress() + ") " + ", monitor time: " + new Date());
                hasRetry = 0;
                LOG.debug("heart beat send to zk.");
            } catch (KeeperException.ConnectionLossException cle) {
                cle.printStackTrace();
                if (hasRetry < maxRetry) {
                    hasRetry++;
                    LOG.warn("Cannot connect to zookeeper, has retryed " + hasRetry + " times, maxRetry " + maxRetry + ".");
                } else {
                    LOG.fatal("Cannot connect to zookeeper, has retryed " + hasRetry + " times, maxRetry " + maxRetry + ", System exit!");
                    System.exit(1);
                }
            } catch (KeeperException.SessionExpiredException see) {
                see.printStackTrace();
                LOG.fatal("Zookeeper Session has expired, try reinit.");
                zk.reinit();
                try {
                    zk.create(hbPath, addr.getHostName() + "(" + addr.getHostAddress() + ") ", CreateMode.EPHEMERAL);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.fatal("Heart beater reinit failed, system exit!");
                    System.exit(1);
                }
            } catch (KeeperException.NoNodeException nne) {
                nne.printStackTrace();
                LOG.fatal("Heart beat failed, node [" + hbPath + "] does not exist!");
                System.exit(1);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    zk.delete(hbPath);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
