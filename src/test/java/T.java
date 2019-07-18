import java.util.concurrent.TimeUnit;
import static  java.lang.System.out;

public class T extends Thread {
    long count = 0;

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            out.println("Dispacher check...");
            out.println(isInterrupted());
            interrupt();
            out.println(isInterrupted());
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
                interrupt();
            }
//            try {
//                SenderEvent se;
//                while ((se = disp.poll(interval, TimeUnit.MILLISECONDS)) != null) {
//                while ((se = disp.poll(5000, TimeUnit.MILLISECONDS)) != null) {
//                    String senderName = se.getSender();
//                    if (senderName == null) {
//                        for (OnealertSender sender : senders) {
//                            while (!sender.offer(se, interval, TimeUnit.MILLISECONDS)) {
//                                LOG.warn(String.format("Dispacher offer senderevent to sender %s failed, retry..l senderevent %s", sender, se));
//                            }
//                        }
//                    } else {
//                        for (OnealertSender sender : senders) {
//                            if (se.getSender().equals(sender.getName())) {
//                                while (!sender.offer(se, interval, TimeUnit.MILLISECONDS)) {
//                                    LOG.warn(String.format("Dispacher offer senderevent to sender %s failed, retry..l senderevent %s", sender, se));
//                                }
//                            }
//                        }
//                    }
//                }
//            } catch (InterruptedException ie) {
//                LOG.warn("Dispacher interrupted.");
//                ie.printStackTrace();
//                interrupt();
//            }
        }
        out.println("Dispacher stopped.");
    }
}
