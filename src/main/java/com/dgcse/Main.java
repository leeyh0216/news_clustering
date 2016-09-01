package com.dgcse;

import com.dgcse.common.LogInstance;
import com.dgcse.entity.Page;
import com.dgcse.entity.Paragraph;
import com.dgcse.entity.WordVector;
import com.dgcse.processor.KMeansProcessor;
import com.dgcse.processor.ParagraphExtractor;
import com.dgcse.processor.Word2VecProcessor;
import com.dgcse.spark.Spark;
import com.dgcse.spark.SparkJDBC;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import scala.Tuple2;

/**
 * Created by leeyh on 2016. 8. 30..
 */
public class Main {
    public static void main(String[] args) throws Exception{

//        JavaRDD<Paragraph> paragraphRDD;
//
//        JavaRDD<Page> pageRDD = SparkJDBC.readTable(Page.TABLE_NAME, Page.getStructType()).map(new Function<Row, Page>() {
//            @Override
//            public Page call(Row v1) throws Exception {
//                return Page.rowToPage(v1);
//            }
//        });
//
//        paragraphRDD = new ParagraphExtractor(pageRDD).pageRddToParagraphRDD();
//        paragraphRDD = paragraphRDD.repartition(Spark.CNT_ALL_CORE);
//        paragraphRDD.cache();
//        paragraphRDD.count();
//
//        Word2VecProcessor word2VecProcessor = new Word2VecProcessor(paragraphRDD);
//        word2VecProcessor.learn();
//
//        paragraphRDD.unpersist();
//
//        for (Tuple2<String, WordVector> stringWordVectorTuple2 : word2VecProcessor.load().take(10)) {
//            LogInstance.getLogger().debug(stringWordVectorTuple2._1 +" : "+stringWordVectorTuple2._2().getVector().get(0));
//        }

//        KMeansProcessor.createSampleData();
        KMeansProcessor kMeansProcessor = new KMeansProcessor();
        kMeansProcessor.init();
        kMeansProcessor.start();
    }
}
