package demo.baluo.monitor.sender;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

public abstract class Sender {
    private Calendar cal = Calendar.getInstance();
    private static Logger LOG = Logger.getLogger(Sender.class);
    private String senderName;
    // 周/月/日/时。
    private List<List<Integer>> workTime = new ArrayList<>();

    public void send(List<SendEvent> ses) {
        if (ses == null)
            return;
        for (SendEvent se : ses) {
            send(se);
        }
    }

    // TODO
    public abstract boolean getAck();

    public abstract void send(SendEvent se);

    public boolean checkActive() {
        cal.setTimeInMillis(System.currentTimeMillis());
        int[] current = new int[4];
        current[0] = (cal.get(Calendar.DAY_OF_WEEK) + 6) % 7;
        current[1] = cal.get(Calendar.MONTH) + 1;
        current[2] = cal.get(Calendar.DAY_OF_MONTH);
        current[3] = cal.get(Calendar.HOUR_OF_DAY);
        for (int i = 0; i < 4; i++) {
            if (!(workTime.get(i).contains(current[i]) || workTime.get(i).get(0) < 0))
                return false;
        }
        return true;
    }

    public void init(String senderName, String workTime) {
        this.senderName = senderName;
        LOG.debug("Sender: " + senderName + ", start to process work time: " + workTime);
        for (String s1 : workTime.split("/")) {
            List<Integer> tmpList = new LinkedList<>();
            if (s1.startsWith("-")) {
                tmpList.add(-1);
            } else {
                for (String s2 : s1.split(",")) {
                    if (s2.contains("-")) {
                        int start = Integer.parseInt(s2.split("-")[0]);
                        int end = Integer.parseInt(s2.split("-")[1]);
                        for (int i = start; i <= end; i++) {
                            tmpList.add(i);
                        }
                    } else {
                        tmpList.add(Integer.parseInt(s2));
                    }
                }
            }
            this.workTime.add(tmpList);
        }
    }

    @Override
    public String toString() {
        return "name: [" +
                senderName +
                "], workTime: [" +
                wt2Str() +
                "]";
    }

    private String wt2Str() {
        StringBuilder sb = new StringBuilder();
        for (List<Integer> l : workTime) {
            sb.append("{");
            for (int s : l) {
                sb.append(s);
                sb.append(",");
            }
            sb.append("}");
        }
        return sb.toString();
    }
}
