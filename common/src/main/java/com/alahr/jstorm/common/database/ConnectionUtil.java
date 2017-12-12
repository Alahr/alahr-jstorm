package com.alahr.jstorm.common.database;

import com.alahr.jstorm.common.properties.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ConnectionUtil {
    private final static Logger logger = LoggerFactory.getLogger(ConnectionUtil.class);

    public static Connection openMysqlConn(String resource){
        PropertiesUtil util = new PropertiesUtil(resource, false);
        Properties properties = util.getProperties();

        String mysqlUrl = properties.getProperty("mysql.url");
        String mysqlUser = properties.getProperty("mysql.user");
        String mysqlPassword = properties.getProperty("mysql.password");
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
        }
        catch (ClassNotFoundException e){
            logger.error("Driver not found", e);
        }
        catch (SQLException e){
            logger.error("open mysql conn exception", e);
        }
        return connection;
    }

    public static void closeMysqlConn(Connection conn){
        try{
            if(null != conn){
                conn.close();
            }
        }
        catch (SQLException e){
            logger.error("close mysql conn exception", e);
        }
    }

    public static void closeStatement(Statement statement){
        try{
            if(null != statement){
                statement.close();
            }
        }
        catch (SQLException e){
            logger.error("close conn statement", e);
        }
    }
}
