package com.dgcse.processor;

import com.dgcse.common.LogInstance;
import com.dgcse.entity.Page;
import com.dgcse.entity.Paragraph;
import com.dgcse.spark.Spark;
import com.dgcse.spark.SparkJDBC;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;

/**
 * Created by leeyh on 2016. 8. 29..
 */
public class Word2VecProcessorTest implements Serializable {

    private JavaRDD<Paragraph> paragraphRDD;

    @Before
    public void init() {
        JavaRDD<Page> pageRDD = SparkJDBC.readTable(Page.TABLE_NAME, Page.getStructType()).map(new Function<Row, Page>() {
            @Override
            public Page call(Row v1) throws Exception {
                return Page.rowToPage(v1);
            }
        }).filter(new Function<Page, Boolean>() {
            @Override
            public Boolean call(Page v1) throws Exception {
                if (v1.getId() < 20000)
                    return true;
                else
                    return false;
            }
        });
        paragraphRDD = new ParagraphExtractor(pageRDD).pageRddToParagraphRDD();
        paragraphRDD.cache();
        paragraphRDD.count();
    }

    @Test
    @Ignore
    public void testLearn() throws Exception{
        long start = System.currentTimeMillis();
        Word2VecProcessor word2VecProcessor = new Word2VecProcessor(paragraphRDD);
        word2VecProcessor.learn();
        long end = System.currentTimeMillis();

        LogInstance.getLogger().debug("duration time : " + ((end - start) / 1000) + " sec");

    }


    @After
    public void destroy() {
        paragraphRDD.unpersist();
        Spark.getJavaSparkContext().close();
    }
}
