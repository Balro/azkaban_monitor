package baluo.monitor.azkaban.sender;

import baluo.monitor.azkaban.util.HttpUtil;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class OnealertSender extends LinkedBlockingQueue<OnealertSender.Event> {
    private static Logger LOG = Logger.getLogger(OnealertSender.class);
    private static String url = "http://api.aiops.com/alert/api/event";
    private String app;
    private boolean running = true;
    private Thread sender;

    public static void setLog(Logger log) {
        LOG = log;
    }

    public Thread getSender() {
        return sender;
    }

    public OnealertSender(String app) {
        super(1000);
        this.app = app;
        sender = new Thread(new Sender());
        sender.start();
        LOG.info(String.format("OneralertSender %s initialized.", app));
    }

    public void close() throws InterruptedException {
        while (!isEmpty()) {
            LOG.debug(String.format("Oneralert %s is closing, remain event %s", app, size()));
            TimeUnit.MILLISECONDS.sleep(1000);
        }
        running = false;
        LOG.info(String.format("Oneralert %s is ready to close.", app));
    }

    private class Sender implements Runnable {
        @Override
        public void run() {
            while (running) {
                try {
                    Event e = OnealertSender.this.poll(10, TimeUnit.SECONDS);
                    if (e == null) {
                        LOG.debug(String.format("OneralertSender %s checked ok.", app));
                        continue;
                    }
                    LOG.info(String.format("Found Event %s.", e));
//                    send(e, true);
                } catch (InterruptedException e) {
                    running = false;
                    e.printStackTrace();
                }
            }
        }

        private void send(Event e, int retry) {
            try {
                String res = HttpUtil.post(url, e.toString());
                JSONObject json = new JSONObject(res);
                if (!json.getString("result").equals("success")) {
                    LOG.error(String.format("OnealertSender %s port suc but res failed. Event %s. Res %s", app, e.toString(), res));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                try {
                    TimeUnit.SECONDS.sleep(10);
                    while (!offer(e, 10, TimeUnit.SECONDS)) {
                        LOG.warn(String.format("OnealertSender %s send event failed, try resend after 10 sec. event %s", app, e.toString()));
                    }
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
        }
    }

    public class Event {
        private String id;
        private String name;
        private String content;


        public Event setId(String id) {
            this.id = id;
            return this;
        }

        public Event setName(String name) {
            this.name = name;
            return this;
        }

        public Event setContent(String content) {
            this.content = content;
            return this;
        }

        public String toString() {
            JSONObject json = new JSONObject();
            json.put("app", app);
            json.put("eventType", "trigger");
            json.put("eventId", id);
            json.put("alarmName", name);
            json.put("alarmContent", content);
            json.put("priority", 2);
            return json.toString();
        }

    }
}
