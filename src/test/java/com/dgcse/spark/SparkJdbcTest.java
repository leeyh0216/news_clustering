package com.dgcse.spark;

import com.dgcse.common.LogInstance;
import com.dgcse.entity.Page;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
/**
 * Created by leeyh on 2016. 8. 27..
 */
public class SparkJdbcTest implements Serializable{
    @Test
    @Ignore
    public void testReadTable(){
        long startTime = System.currentTimeMillis();
        long cnt = SparkJDBC.readTable(Page.TABLE_NAME,Page.getStructType()).count();
        long endTime = System.currentTimeMillis();

        LogInstance.getLogger().debug("count of page : " + cnt);
        LogInstance.getLogger().debug("read time : "+((endTime-startTime)/1000)+" sec");
    }
}
