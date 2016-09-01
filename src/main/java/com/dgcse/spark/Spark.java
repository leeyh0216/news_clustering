package com.dgcse.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by leeyh on 2016. 8. 27..
 */
public class Spark implements Serializable{
    private static JavaSparkContext javaSparkContext;
    public static SparkConf sparkConf;
    public static final String APP_NAME = "NEWS_CLUSTERING_TRANSFORMER";
    public static final String MASTER = "spark://172.27.0.99:7077";
    public static int CNT_ALL_CORE = 16;
    public static final String HDFS_HOST = "hdfs://dgucse-bullet01:9000";

    static{
        sparkConf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER);
        sparkConf.set("spark.default.parallelism", String.valueOf(CNT_ALL_CORE));
        sparkConf.set("spark.cores.max",String.valueOf(CNT_ALL_CORE));
        sparkConf.set("spark.driver.memory","16g");
        sparkConf.set("spark.executor.memory","16g");
        sparkConf.set("spark.executor.cores","8");
        javaSparkContext = new JavaSparkContext(sparkConf);
    }

    public static JavaSparkContext getJavaSparkContext(){
        return javaSparkContext;
    }
}
