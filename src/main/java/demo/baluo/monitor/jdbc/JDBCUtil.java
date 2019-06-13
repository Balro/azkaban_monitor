package demo.baluo.monitor.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCUtil {
    private Properties prop;
    private String module;
    private String dbhost;
    private String dbport;
    private String dbuser;
    private String dbpasswd;
    private String dbname;
    private String connURL;
    private Connection conn;

    public JDBCUtil(Properties prop) {
        this.prop = prop;
    }

    public Connection getConn() {
        try {
            if (conn == null || conn.isClosed()) {
                conn = DriverManager.getConnection(connURL, dbuser, dbpasswd);
                return conn;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public JDBCUtil init() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        this.dbhost = prop.getProperty(module + "dbhost");
        this.dbport = prop.getProperty(module + "dbport");
        this.dbuser = prop.getProperty(module + "dbuser");
        this.dbpasswd = prop.getProperty(module + "dbpasswd");
        this.dbname = prop.getProperty(module + "dbname");
        connURL = "jdbc:mysql://" + dbhost + ":" + dbport + "/" + dbname;
        return this;
    }

    public JDBCUtil init(String module) throws Exception{
        this.module = module + ".";
        return init();
    }

    public void close() throws Exception {
        if (!conn.isClosed()) {
            conn.close();
        }
    }
}
