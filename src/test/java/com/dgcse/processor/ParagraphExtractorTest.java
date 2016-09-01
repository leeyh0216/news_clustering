package com.dgcse.processor;

import com.dgcse.entity.Page;
import com.dgcse.entity.Paragraph;
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
public class ParagraphExtractorTest implements Serializable {

    /**
     * 첫번째 기사(id:1)에 대하여 테스트 진행.
     * 제목 : [TV쪼개기] ‘왕의얼굴’ 서인국, 세상 가장 불쌍한 왕위계승자.
     * 총 22개의 단락으로 이루어져 있음.
     */
    @Test
    @Ignore
    public void testPageToParagraphRDD(){
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

        List<Paragraph> paragraphList = new ParagraphExtractor(pageRDD).pageRddToParagraphRDD().collect();
        assertNotEquals(paragraphList.size(),22);
    }

    @After
    public void destroy(){
        Spark.getJavaSparkContext().close();
    }
}
