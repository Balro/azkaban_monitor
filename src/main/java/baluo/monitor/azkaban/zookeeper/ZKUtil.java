package baluo.monitor.azkaban.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Properties;

public class ZKUtil implements Watcher {
    private static final Logger LOG = Logger.getLogger(ZKUtil.class);
    private Properties prop;
    private ZooKeeper zoo;
    private String zkroot;
    private String connString;
    int timeout;

    public String getZkroot() {
        return zkroot;
    }

    public ZKUtil(Properties prop) {
        this.prop = prop;
    }

    public ZKUtil init() {
        return init(null);
    }

    public ZKUtil init(String module) {
        connString = prop.getProperty("zookeeper.connString");
        timeout = Integer.parseInt(prop.getProperty("zookeeper.timeout"));
        zkroot = prop.getProperty("zookeeper.node");
        if (module != null) {
            zkroot = zkroot + "/" + module;
        }
        try {
            zoo = new ZooKeeper(connString, timeout, this);
            String[] paths = zkroot.split("/");
            String tmp = "";
            for (int i = 1; i < paths.length; i++) {
                tmp = tmp + "/" + paths[i];
                if (!exist(tmp)) {
                    create(tmp, "null", CreateMode.PERSISTENT);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.fatal("ZKUtil init failed, System exit!");
            System.exit(1);
        }
        return this;
    }

    public void reinit() {
        try {
            zoo = new ZooKeeper(connString, timeout, this);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            LOG.fatal("ZKUtil reinit failed, system exit!");
            System.exit(1);
        }
    }

    public void create(String path, String data, CreateMode mode) throws InterruptedException, KeeperException {
        zoo.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, mode);
    }

    public boolean delete(String path) throws Exception {
        zoo.delete(path, -1);
        return false;
    }

    public boolean exist(String path) throws Exception {
        Stat s = zoo.exists(path, this);
        return s != null;
    }

    public String getData(String path) throws Exception {
        return new String(zoo.getData(path, false, null));
    }

    public Stat getState(String path) throws Exception {
        return zoo.exists(path, this);
    }

    public void setData(String path, String data) throws Exception {
        zoo.setData(path, data.getBytes(), -1);
    }

    public void close() throws Exception {
        zoo.close();
    }

    @Override
    public void process(WatchedEvent event) {

    }
}
