package com.dgcse.processor;

import com.dgcse.entity.Page;
import com.dgcse.entity.Paragraph;
import com.dgcse.spark.Spark;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.List;

/**
 * Created by leeyh on 2016. 8. 28..
 */
public class ParagraphExtractor implements Serializable {

    private JavaRDD<Page> pageRDD;

    public ParagraphExtractor(JavaRDD<Page> pageRDD){
        this.pageRDD = pageRDD;
    }

    /**
     * PageRDD를 ParagraphRDD로 변환한다.
     * @return ParagraphRDD
     */
    public JavaRDD<Paragraph> pageRddToParagraphRDD(){
        return pageRDD.repartition(Spark.CNT_ALL_CORE).flatMap(new FlatMapFunction<Page, Paragraph>() {
            @Override
            public Iterable<Paragraph> call(Page page) throws Exception {
                String body = page.getBody();
                int id = page.getId();

                body = body.replaceAll("\n","");
                List<Paragraph> paragraphList = Lists.newArrayList();

                String[] paragraphArr = body.split("\\.|\\?|!");

                for (String s : paragraphArr)
                    paragraphList.add(new Paragraph(id,s));
                return paragraphList;
            }
        });
    }
}
