import balro.monitor.azkaban.AzkabanMonitor;
import balro.monitor.azkaban.util.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class AzkabanMonitorTest {
    @Test
    public void AzkabanMonitorRunTest() throws Exception {
        AzkabanMonitor.main(new String[]{"start"});
        TimeUnit.SECONDS.sleep(10);
        AzkabanMonitor.main(new String[]{"status"});
        TimeUnit.SECONDS.sleep(10);
        ZooKeeper zk = new ZooKeeper("test01", 5000, ZKUtil.EMPTY_WATCHER);
        ZKUtil.delete(zk, "/monitor");
        zk.close();
        TimeUnit.SECONDS.sleep(120);
    }

    public static void main(String[] args) {
        AzkabanMonitor.main(new String[]{"start"});
    }
}
