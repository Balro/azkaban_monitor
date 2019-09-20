package balro.monitor.azkaban.sender;

import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class OnealertSenderTest {
    static {
        OneAlertSender.setLog(Logger.getLogger("test"));
    }


//    @Test
    public void sendTest() throws Exception {
        OneAlertSender os = new OneAlertSender("test", "abc", 10000, null, null);
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
