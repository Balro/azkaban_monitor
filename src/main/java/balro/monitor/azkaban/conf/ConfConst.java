package balro.monitor.azkaban.conf;

public class ConfConst {
    public static final String ZOO_QUORUM = "zookeeper.quorum";
    public static final String ZOO_QUORUM_DEF = "zk01:2181,zk02:2181,zk03:2181";
    public static final String ZOO_NODE = "zookeeper.node";
    public static final String ZOO_NODE_DEF = "/tmp";
    public static final String ZOO_TIMEOUT_MILLI = "zookeeper.timeout";
    public static final int ZOO_TIMEOUT_MILLI_DEF = 90000;
    public static final String MONITOR_HEART_BEAT_INTERVAL_MILLI = "monitor.heartbeat.interval";
    public static final int MONITOR_HEART_BEAT_INTERVAL_MILLI_DEF = 30000;
    public static final String DATABASE_DRIVER = "database.driver";
    public static final String DATABASE_DRIVER_DEF = "com.mysql.jdbc.Driver";
    public static final String DATABASE_URL = "database.url";
    public static final String DATABASE_USER = "database.user";
    public static final String DATABASE_PASSWORD = "database.password";
    public static final String CONDITIONS = "conditions";
    public static final String SENDERS = "senders";
}
