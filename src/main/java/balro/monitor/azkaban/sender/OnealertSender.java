package balro.monitor.azkaban.sender;

import balro.monitor.azkaban.util.HttpUtil;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class OnealertSender extends LinkedBlockingQueue<SenderEvent> implements Runnable {
    private static Logger LOG = Logger.getLogger(OnealertSender.class);
    private static String url = "http://api.aiops.com/alert/api/event";
    private String app;
    private String name;
    private int interval;
    private ArrayList<Integer> week = new ArrayList<>();
    private ArrayList<Integer> hour = new ArrayList<>();
    private Calendar cal = Calendar.getInstance();

    public static void setLog(Logger log) {
        LOG = log;
    }

    public OnealertSender(String name, String app, int interval, String week, String hour) {
        super(1000);
        this.name = name;
        this.app = app;
        this.interval = interval;
        initTime(this.week, week);
        initTime(this.hour, hour);
        LOG.info(String.format("OneralertSender %s initialized, week=%s, hour=%s.", app, this.week.toString(), this.hour.toString()));
    }

    private void initTime(ArrayList<Integer> time, String timeStr) {
        if (timeStr == null) return;
        String[] s1 = timeStr.split(",");
        for (String s : s1) {
            String[] s2 = s.split("-");
            if (s2.length > 1) {
                int start = Integer.parseInt(s2[0].trim());
                int end = Integer.parseInt(s2[1].trim());
                for (int i = start; i <= end; i++) {
                    time.add(i);
                }
            } else {
                time.add(Integer.parseInt(s2[0].trim()));
            }
        }
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            LOG.debug(String.format("OnealertSender %s/%s regularly check.", name, app));
            try {
                SenderEvent se;
                while ((se = this.poll(interval, TimeUnit.MILLISECONDS)) != null) {
                    if (shouldSend()) {
                        send(se, 2);
                        LOG.info(String.format("%s/%s found Event %s and send.", name, app, se));
                    } else {
                        LOG.debug(String.format("%s/%s found Event %s but not in sendtime.", name, app, se));
                    }
                }
            } catch (InterruptedException e) {
                LOG.warn(String.format("OnealertSender %s/%s interrupted.", name, app));
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
        LOG.info(String.format("OnealertSender %s/%s stopped.", name, app));
    }

    private boolean shouldSend() {
        return checkWeek() && checkHour();
    }

    private boolean checkWeek() {
        if (week.size() == 0) {
            return true;
        }
        cal.setTimeInMillis(System.currentTimeMillis());
        int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
        w = w == 0 ? 7 : w;
        return week.contains(w);
    }

    private boolean checkHour() {
        if (hour.size() == 0) {
            return true;
        }
        cal.setTimeInMillis(System.currentTimeMillis());
        int h = cal.get(Calendar.HOUR_OF_DAY);
        return hour.contains(h);
    }

    private void send(SenderEvent se, int retry) {
        JSONObject data = new JSONObject();
        data.put("app", app);
        data.put("eventId", String.format("%s/%s/%s/%s/%s", se.getExecId(), se.getProject(), se.getFlow(), se.getJob(), se.getAttempt()));
        data.put("eventType", "trigger");
        data.put("alarmName", String.format("%s/%s/%s/%s/%s", se.getExecId(), se.getProject(), se.getFlow(), se.getJob(), se.getAttempt()));
        data.put("priority", 1);
        data.put("alarmContent", se.getMsg());

        int retried = 0;
        while (retried <= retry) {
            try {
                String res = HttpUtil.post(url, data.toString());
                JSONObject json = new JSONObject(res);
                if (json.getString("result").equals("success")) {
                    LOG.info(String.format("OnealertSender %s/%s send success. Event %s. Res %s", name, app, se.toString(), res));
                } else {
                    LOG.error(String.format("OnealertSender %s/%s send event succ bug get failed return msg. Event %s. returnMsg %s", name, app, se.toString(), res));
                }

                return;
            } catch (Exception ex) {
                ex.printStackTrace();
                try {
                    TimeUnit.MILLISECONDS.sleep(interval);
                    LOG.warn(String.format("OnealertSender %s send event failed, try resend after 10 sec. event %s", app, se.toString()));
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
            retried++;
        }
    }

    public String getName() {
        return name;
    }
}
