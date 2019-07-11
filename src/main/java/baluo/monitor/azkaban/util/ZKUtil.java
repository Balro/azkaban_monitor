package baluo.monitor.azkaban.util;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;

public class ZKUtil {
    private static Logger LOG = Logger.getLogger(ZKUtil.class);

    public static void setLOG(Logger log) {
        LOG = log;
    }

    public static void create(ZooKeeper zoo, String path, String data, CreateMode mode) throws InterruptedException, KeeperException {
        zoo.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
        LOG.debug(String.format("create %s, data %s, mode %s", path, data, mode));
    }

    public static void delete(ZooKeeper zoo, String path) throws Exception {
        zoo.delete(path, -1);
        LOG.debug(String.format("delete %s", path));
    }

    public static boolean exist(ZooKeeper zoo, String path) throws KeeperException, InterruptedException {
        Stat stat = zoo.exists(path, false);
        return stat != null;
    }

    public static String get(ZooKeeper zoo, String path) throws KeeperException, InterruptedException {
        return Arrays.toString(zoo.getData(path, false, null));
    }

    public static void set(ZooKeeper zoo, String path, String data) throws KeeperException, InterruptedException {
        zoo.setData(path, data.getBytes(), -1);
    }

    public static Watcher EMPTY_WATCHER = watchedEvent -> {
    };
}
