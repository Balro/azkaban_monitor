import baluo.monitor.azkaban.util.ZKUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;

public class ZKUtilTest {
    private ZooKeeper zoo;
    private String quorum = "test01:2181";
    private String node = "/ZKUtilTest" + System.currentTimeMillis();

    static {
        ZKUtil.setLOG(Logger.getLogger("test"));
    }

    @Test
    public void createTest() throws Exception {
        zoo = newZoo();
        ZKUtil.create(zoo, node, "hello", CreateMode.EPHEMERAL);
        Stat stat = zoo.exists(node, false);
        Assert.assertNotNull(stat);
        zoo.close();
    }

    @Test
    public void deleteTest() throws Exception {
        zoo = newZoo();
        zoo.create(node, "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Stat stat = zoo.exists(node, false);
        Assert.assertNotNull(stat);
        ZKUtil.delete(zoo, node);
        stat = zoo.exists(node, false);
        Assert.assertNull(stat);
        zoo.close();
    }

    private ZooKeeper newZoo() throws Exception {
        return new ZooKeeper(quorum, 90000, ZKUtil.EMPTY_WATCHER);
    }
}
