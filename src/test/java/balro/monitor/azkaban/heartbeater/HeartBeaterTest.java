package balro.monitor.azkaban.heartbeater;

import balro.monitor.azkaban.util.ZKUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.TimeUnit;

public class HeartBeaterTest {
    static {
        HeartBeater.setLOG(Logger.getLogger("test"));
    }

    public static void main(String[] args) throws Exception {
        String zoo = "test01:2181";
        String node = "/tmp";
        HeartBeater hb = new HeartBeater(zoo, 6000, node, 1000);
        Thread t = new Thread(hb);
        t.start();
        TimeUnit.SECONDS.sleep(5);
        ZooKeeper zk = new ZooKeeper(zoo, 5000, ZKUtil.EMPTY_WATCHER);
        ZKUtil.delete(zk, node);
        zk.close();
        TimeUnit.SECONDS.sleep(120);
    }
}
