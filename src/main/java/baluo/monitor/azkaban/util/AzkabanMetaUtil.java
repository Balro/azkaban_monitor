package baluo.monitor.azkaban.util;

import org.apache.commons.dbcp.BasicDataSource;

public class AzkabanMetaUtil {
    private static BasicDataSource bds;

    private static void init(String url, String user, String passwd) {
        bds = new BasicDataSource();
        bds.setDriverClassName("com.mysql.jdbc.Driver");
        bds.setUrl(url);
        bds.setUsername(user);
        bds.setPassword(passwd);
        bds.setInitialSize(1);
        bds.setMaxActive(10);
        bds.setMinIdle(1);
        bds.setMaxIdle(10);
        bds.setMinEvictableIdleTimeMillis(1000 * 60 * 30);
    }
}
