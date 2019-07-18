import org.apache.commons.configuration.XMLConfiguration;

import static java.lang.System.out;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.*;

public class Test {

    public static void main(String[] args) throws Exception {
//        ArrayList<Integer> time = new ArrayList<>();
//        String timeStr = "2,4,6,7";
//        String[] s1 = timeStr.split(",");
//        for (String s : s1) {
//            String[] s2 = s.split("-");
//            if (s2.length > 1) {
//                int start = Integer.parseInt(s2[0].trim());
//                int end = Integer.parseInt(s2[1].trim());
//                for (int i = start; i <= end; i++) {
//                    time.add(i);
//                }
//            } else {
//                time.add(Integer.parseInt(s2[0].trim()));
//            }
//        }
//        out.println(time.toString());
        XMLConfiguration conf = new XMLConfiguration();
        conf.setDelimiterParsingDisabled(true);
        conf.load("azkaban-monitor.xml");
        List s =  conf.getList("senders.qinghe.week");
        out.println(s);
        out.println(s);
        for (Object o : s) {
            out.println(o.toString());
        }
    }
}
