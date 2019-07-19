package balro.monitor.azkaban.sender;

import balro.monitor.azkaban.sender.OnealertSender;
import balro.monitor.azkaban.sender.SenderEvent;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class OnealertSenderTest {
    static {
        OnealertSender.setLog(Logger.getLogger("test"));
    }


    @Test
    public void sendTest() throws Exception {
        OnealertSender os = new OnealertSender("test", "abc", 10000, null, null);
        new Thread(os).start();
        TimeUnit.SECONDS.sleep(5);
        SenderEvent se = new SenderEvent();
        se.setExecId(1);
        se.setMsg("hello");
        System.out.println("Offer a event.");
        os.offer(se);
        TimeUnit.SECONDS.sleep(5);
    }
}
