package balro.monitor.azkaban.dispatcher;

import balro.monitor.azkaban.conf.ConfConst;
import balro.monitor.azkaban.sender.OnealertSender;
import balro.monitor.azkaban.sender.SenderEvent;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Dispacher extends Thread {
    private static Logger LOG = Logger.getLogger(Dispacher.class);
    private XMLConfiguration conf;
    private List<OnealertSender> senders = new ArrayList<>();
    private int interval;
    private LinkedBlockingQueue<SenderEvent> disp = new LinkedBlockingQueue<>(1000);

    public Dispacher(XMLConfiguration conf) {
        interval = conf.getInt(ConfConst.MONITOR_HEART_BEAT_INTERVAL_MILLI, ConfConst.MONITOR_HEART_BEAT_INTERVAL_MILLI_DEF);
        this.conf = conf;
    }

    public void addSender(OnealertSender sender) {
        senders.add(sender);
        LOG.info(String.format("Dispacher add sender %s", sender.getName()));
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            LOG.debug("Dispacher check regularly.");
            try {
                SenderEvent se;
                while ((se = disp.poll(interval, TimeUnit.MILLISECONDS)) != null) {
                    LOG.info(String.format("Dispacher get event: %s.", se));
                    String senderName = se.getSender();
                    if (senderName == null || senderName.length() == 0) {
                        for (OnealertSender sender : senders) {
                            while (!sender.offer(se, interval, TimeUnit.MILLISECONDS)) {
                                LOG.warn(String.format("Dispacher offer senderevent to sender %s failed, retry..l senderevent %s", sender, se));
                            }
                            LOG.info(String.format("Dispacher offer event %s to sender %s.", se, sender.getName()));
                        }
                    } else {
                        for (OnealertSender sender : senders) {
                            if (se.getSender().equals(sender.getName())) {
                                while (!sender.offer(se, interval, TimeUnit.MILLISECONDS)) {
                                    LOG.warn(String.format("Dispacher offer senderevent to sender %s failed, retry..l senderevent %s", sender, se));
                                }
                                LOG.info(String.format("Dispacher offer event %s to sender %s.", se, sender.getName()));
                            }
                        }
                    }
                }
            } catch (InterruptedException ie) {
                LOG.warn("Dispacher interrupted.");
                ie.printStackTrace();
                interrupt();
            }
        }
        LOG.warn("Dispacher stopped.");
    }

    public LinkedBlockingQueue<SenderEvent> getDisp() {
        return disp;
    }
}
