package com.dgcse.spark;



import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Properties;

/**
 * Created by leeyh on 2016. 8. 27..
 */
public class SparkJDBC implements Serializable {
    private static final String JDBC_PREFIX = "jdbc:mysql://";
    private static final String DB_HOST = "172.27.0.99";
    private static final String DB_PORT = "3306";
    private static final String DB_NAME="news_db";
    public static final String DB_USER = "root";
    public static final String DB_PW = "";
    public static Properties SQL_PROPERTIES;
    public static String DB_URL;
    static{
        initSqlProperties();
    }

    /**
     * SQL 정보를 초기화한다.
     */
    private static void initSqlProperties(){
        SQL_PROPERTIES = new Properties();

        DB_URL = JDBC_PREFIX+DB_HOST+":"+DB_PORT+"/"+DB_NAME;
        SQL_PROPERTIES.setProperty("url",DB_URL);
        SQL_PROPERTIES.setProperty("user",DB_USER);
        SQL_PROPERTIES.setProperty("password",DB_PW);
        SQL_PROPERTIES.setProperty("driver","com.mysql.jdbc.Driver");
    }
    /**
     * JDBC에 연결된 SQL Context를 리턴한다.
     * @return
     */
    public static SQLContext getSQLContext(){
        return new SQLContext(Spark.getJavaSparkContext());
    }

    /**
     * MySQL Table을 READ하여 리턴한다
     * @param tableName Read할 Table명
     * @param structType 테이블 구조
     * @return Read한 Data(JavaRDD)
     */
    public static JavaRDD<Row> readTable(String tableName, StructType structType){
        return getSQLContext().read().schema(structType).jdbc(DB_URL,tableName,SQL_PROPERTIES).toJavaRDD();
    }
}
