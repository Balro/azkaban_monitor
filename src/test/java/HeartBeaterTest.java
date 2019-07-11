import baluo.monitor.azkaban.heartbeater.HeartBearter2;
import org.apache.log4j.Logger;
import org.junit.Test;

public class HeartBeaterTest {
    static {
        HeartBearter2.setLOG(Logger.getLogger("test"));
    }

    @Test
    public void startTest() {
//        HeartBearter2 hb = new HeartBearter2("test01:2181");
    }
}
