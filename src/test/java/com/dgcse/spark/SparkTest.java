package com.dgcse.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by leeyh on 2016. 8. 27..
 */
public class SparkTest {
    @Test
    @Ignore
    public void testInitJavaSparkContext(){
        JavaSparkContext jsc = Spark.getJavaSparkContext();
        assertNotNull(jsc);

        jsc.close();
    }
}
