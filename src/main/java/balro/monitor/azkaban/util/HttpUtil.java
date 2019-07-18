package balro.monitor.azkaban.util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpUtil {
    private static Logger LOG = Logger.getLogger(HttpUtil.class);

    public static void setLog(Logger log) {
        LOG = log;
    }

    public static String post(String urlStr, String data) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setUseCaches(false);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("content-Type", "application/json");
        conn.setRequestProperty("charset", "utf-8");
        conn.setRequestProperty("Content-length", String.valueOf(data.getBytes().length));

        DataOutputStream dostream = null;
        BufferedReader breader = null;
        int resCode = -1;
        try {
            dostream = new DataOutputStream(conn.getOutputStream());
            dostream.write(data.getBytes());
            dostream.flush();
            resCode = conn.getResponseCode();

            StringBuilder sb = new StringBuilder();

            breader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String tmp;
            while ((tmp = breader.readLine()) != null) {
                sb.append(tmp);
            }
            if (resCode > 200) {
                LOG.error(String.format("POST failed, return code %s, POST msg %s", resCode, data));
            } else {
                LOG.info(String.format("POST succeed, data %s, return msg: %s", data, sb.toString()));
            }
            return sb.toString();
        } finally {
            if (dostream != null)
                dostream.close();
            if (breader != null)
                breader.close();
        }
    }
}
