package baluo.monitor.azkaban.conf;

public class Configuration {
    public static final String ZOO_QUORUM = "zookeeper.quorum";
    public static final String ZOO_QUORUM_DEF = "zk01:2181,zk02:2181,zk03:2181";
    public static final String ZOO_NODE = "zookeeper.root";
    public static final String ZOO_NODE_DEF = "/tmp";
    public static final String ZOO_TIMEOUT_MILLI = "zookeeper.timeout";
    public static final int ZOO_TIMEOUT_MILLI_DEF = 90000;
    public static final String MONITOR_HEART_BEAT_INTERVAL_MILLI = "monitor.heartbeat.interval";
    public static final int MONITOR_HEART_BEAT_INTERVAL_MILLI_DEF = 30000;
    public static final String AZKABAN_DB_HOST = "azkaban.dbhost";
    public static final String AZKABAN_DB_PORT = "azkaban.dbport";
    public static final String AZKABAN_DB_USER = "azkaban.dbuser";
    public static final String AZKABAN_DB_PASSWD = "azkaban.dbpasswd";
    public static final String AZKABAN_ALERT = "azkaban.alert";
    public static final String SENDERS = "senders";
}
