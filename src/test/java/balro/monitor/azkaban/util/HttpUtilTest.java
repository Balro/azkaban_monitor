package balro.monitor.azkaban.util;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class HttpUtilTest {
    static {
        HttpUtil.setLog(Logger.getLogger("test"));
    }

    @Test
    public void postTest() throws Exception {
        String app = "abc";
        JSONObject json = new JSONObject();
        json.put("app", app);
        json.put("eventId", "123");
        json.put("eventType", "trigger");
        json.put("alarmName", "testName");
        json.put("priority", 1);
        json.put("alarmContent", "testContent");
        try {
            HttpUtil.post("http://api.aiops.com/alert/api/event", json.toString());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertNull(e);
        }
    }
}
