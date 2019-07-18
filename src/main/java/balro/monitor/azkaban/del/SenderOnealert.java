package balro.monitor.azkaban.del;

import balro.monitor.azkaban.sender.SenderEvent;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class SenderOnealert extends Sender {
    private static Logger LOG = Logger.getLogger(SenderOnealert.class);
    private String urlString = "http://api.aiops.com/alert/api/event";
    private String app;

    public SenderOnealert(String app) {
        this.app = app;
    }

    @Override
    public boolean getAck() {
        // TODO
        return false;
    }

    @Override
    public void send(SenderEvent se) {
//        LOG.info("Send event: " + se);
//        try {
//            if (postJson(getJsonObj(se))) {
//                LOG.info("Send event succeed: " + se);
//            } else {
//                LOG.error("Send event failed: " + se);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    private boolean postJson(JSONObject jObj) throws Exception {
        String data = jObj.toString();
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setUseCaches(false);

        conn.setRequestMethod("POST");
        conn.setRequestProperty("content-Type", "application/json");
        conn.setRequestProperty("charset", "utf-8");
        conn.setRequestProperty("Content-length", String.valueOf(data.getBytes().length));


        DataOutputStream dos = null;
        BufferedReader br = null;
        int resCode = -1;
        try {
            dos = new DataOutputStream(conn.getOutputStream());
            dos.write(data.getBytes());
            dos.flush();
            resCode = conn.getResponseCode();
            if (resCode > 200) {
                LOG.error(" POST operation failed, return code [" + resCode + "], POST msg: " + data);
            }

            StringBuilder sb = new StringBuilder();

            br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String tmp;
            while ((tmp = br.readLine()) != null) {
                sb.append(tmp);
            }
            String result = new JSONObject(sb.toString()).getString("result");
            if (result.equals("success")) {
                LOG.info("Send event succeed, return msg: " + sb.toString());
                return true;
            } else {
                LOG.error("Send event failed, return msg: " + sb.toString() + ", POST msg: " + data);
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dos != null)
                dos.close();
            if (br != null)
                br.close();
        }
        return false;
    }

    /**
     * 参数 必须  备注
     * app 必须  需要告警集成的应用KEY
     * eventType   必须  触发告警trigger，解决告警resolve
     * alarmName   trigger必须, resolve可选    告警标题，故障简述  // 一般不会直接显示，因此告警标题和内容考虑卸载alarmContent和detail字段。
     * eventId 必须  外部事件id，告警关闭时用到  // 必须保证不同告警事件拥有不同的id，否则会导致后续告警被合并而无法触发。
     * alarmContent    可选  告警详情
     * entityName  可选  告警对象名
     * entityId    可选  告警对象id
     * priority    可选  提醒 1，警告 2，严重 3 // 一般设置为3。
     * details 可选  详情  // 必须为 "details: {"key":"value"}" 形式。
     * contexts    可选  上下文
     *
     * @param es
     * @return
     */
//    private JSONObject getJsonObj(SenderEvent es) {
//        JSONObject jObj = new JSONObject();
//        jObj.put("app", app);
//        jObj.put("eventType", "trigger");
//        jObj.put("alarmName", es.title);
//        jObj.put("eventId", es.id);
//        jObj.put("alarmContent", es.msg);
//        jObj.put("priority", 2);
//        jObj.put("details", new JSONObject("{detail:" + es.detail + "}"));
//        return jObj;
//    }

}
