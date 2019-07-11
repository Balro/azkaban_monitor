import baluo.monitor.azkaban.sender.OnealertSender;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class OnealertSenderTest {
    static {
        OnealertSender.setLog(Logger.getLogger("test"));
    }

    private String testApp = "abdc623d-c3e3-a908-05d5-a5e0cfca62d3";

    @Test
    public void closeTest() throws Exception {
        OnealertSender os = new OnealertSender(testApp);
        TimeUnit.SECONDS.sleep(5);
        Assert.assertTrue(os.getSender().isAlive());
        os.close();
        TimeUnit.SECONDS.sleep(5);
        Assert.assertFalse(os.getSender().isAlive());
    }

    @Test
    public void sendTest() throws Exception {
        OnealertSender os = new OnealertSender(testApp);
        os.offer(os.new Event().setName("test"));
        TimeUnit.SECONDS.sleep(5);
    }
}
