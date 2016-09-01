package com.dgcse.processor;

import com.dgcse.common.LogInstance;
import com.dgcse.entity.Page;
import com.dgcse.entity.Word;
import com.dgcse.spark.Spark;
import com.dgcse.spark.SparkJDBC;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.List;

/**
 * Created by leeyh on 2016. 8. 28..
 */
public class WordExtractorTest implements Serializable{
    @Test
    @Ignore
    public void testPageRddToWordRDD(){
        JavaRDD<Page> pageRDD = SparkJDBC.readTable(Page.TABLE_NAME,Page.getStructType()).map(new Function<Row, Page>() {
            @Override
            public Page call(Row v1) throws Exception {
                return Page.rowToPage(v1);
            }
        }).filter(new Function<Page, Boolean>() {
            @Override
            public Boolean call(Page v1) throws Exception {
                if(v1.getId()==1)
                    return true;
                else
                    return false;
            }
        });

        WordExtractor wordExtractor = new WordExtractor(pageRDD);
        JavaRDD<Word> wordRDD = wordExtractor.pageRddToWordRdd();
        wordRDD.cache();
        double allCnt = 163;
        double kwanghaeCnt = 12;
        double expect = kwanghaeCnt/allCnt;

        List<Word> wordList = wordRDD.filter(new Function<Word, Boolean>() {
            @Override
            public Boolean call(Word v1) throws Exception {
                if(v1.getWord().equals("광해"))
                    return true;
                else
                    return false;
            }
        }).collect();

        Word kwanghaeWord = wordList.get(0);
        LogInstance.getLogger().debug("expected : "+expect+", actual : "+kwanghaeWord.getTf());
        assertEquals(expect,kwanghaeWord.getTf(),0.05);
    }

    @After
    public void destroy(){
        Spark.getJavaSparkContext().close();
    }
}
