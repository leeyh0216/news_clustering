package com.dgcse.processor;

import com.dgcse.common.LogInstance;
import com.dgcse.entity.Paragraph;
import com.dgcse.entity.WordVector;
import com.dgcse.spark.Spark;
import com.dgcse.spark.SparkJDBC;
import com.twitter.penguin.korean.KoreanPosJava;
import com.twitter.penguin.korean.KoreanTokenJava;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by leeyh on 2016. 8. 29..
 */
public class Word2VecProcessor implements Serializable{
    private JavaRDD<Paragraph> paragraphRDD;
    private static StructType schema;
    private Word2Vec word2Vec;
    private static final int MINIMUM_LINE_LENGTH = 5;
    private static final int MINIMUM_WORD_CNT = 2;
    private static final String WORD2VEC_MODEL_DIR = "/user/root/word2vec/word2vec.model";
    public static final int VECTOR_SIZE = 200;
    //Schema를 초기화한다.
    static{
        schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
    }

    /**
     * Word2Vec 학습 시 사용하는 생성자.
     * @param paragraphRDD 문장 RDD
     */
    public Word2VecProcessor(JavaRDD<Paragraph> paragraphRDD){
        this.paragraphRDD = paragraphRDD;
    }

    /**
     * WordVector 로드 시 사용할 생성자
     */
    public Word2VecProcessor(){

    }

    /**
     * Line을 Stemming하여 String Array로 변환한다.
     * @param line Stemming할 Line
     * @return Stemed Word Array
     */
    private String[] lineToWordArray(String line){
        if(line.length()<MINIMUM_LINE_LENGTH)
            return new String[0];
        List<KoreanTokenJava> list = WordExtractor.stemmingLine(line);

        if(list.size()==0)
            return new String[0];

        String str = "";

        for (KoreanTokenJava koreanTokenJava : list) {
            //고유명사와 명사만을 걸러낸다.
            if ((koreanTokenJava.getPos().equals(KoreanPosJava.ProperNoun) || koreanTokenJava.getPos().equals(KoreanPosJava.Noun)) && koreanTokenJava.getLength() > 1)
                str+=koreanTokenJava.getText()+" ";
        }

        return str.split(" ");
    }

    /**
     * ParagraphRDD를 RowRDD로 변환한다.
     * @return Stemming된 Line을 가지는 RowRDD
     */
    private JavaRDD<Row> paragraphRddToRowRdd(){
        return paragraphRDD.repartition(Spark.CNT_ALL_CORE).map(new Function<Paragraph, String[]>() {
            @Override
            public String[] call(Paragraph v1) throws Exception {
                return lineToWordArray(v1.getLine());
            }
        }).filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] v1) throws Exception {
                //최소 단어 갯수를 만족하지 않는다면 학습 대상에서 제외한다.
                if(v1.length< MINIMUM_WORD_CNT)
                    return false;
                else
                    return true;
            }
        }).map(new Function<String[], Row>() {
            @Override
            public Row call(String[] v1) throws Exception {
                return RowFactory.create(Arrays.asList(v1));
            }
        });
    }

    /**
     * 하둡 디렉토리 내에 Word2Vec model이 존재하는지 확인한다.
     * @return
     * @throws IOException
     */
    private boolean isFileExist() throws IOException{
        Configuration hdfsConf = new Configuration();
        hdfsConf.set("fs.default.name",Spark.HDFS_HOST);
        FileSystem fs = FileSystem.get(hdfsConf);
        return fs.exists(new Path(WORD2VEC_MODEL_DIR));

    }

    /**
     * Word2Vec Model을 학습한다.
     * @throws IOException
     */
    public void learn() throws IOException{
        //파일이 이미 존재한다면 학습하지 않는다.
        if(isFileExist()) {
            LogInstance.getLogger().debug("file exist so shutdown");
            return;
        }

        //Word2Vec 모델을 학습한다.
        DataFrame stemmedLineDataSet = SparkJDBC.getSQLContext().createDataFrame(paragraphRddToRowRdd(),schema);
        word2Vec = new Word2Vec().setNumPartitions(Spark.CNT_ALL_CORE).setInputCol("text").setOutputCol("result").setVectorSize(VECTOR_SIZE).setMaxIter(10).setWindowSize(5).setMinCount(3);
        Word2VecModel w2model = word2Vec.fit(stemmedLineDataSet);
        w2model.getVectors().write().json(Spark.HDFS_HOST+WORD2VEC_MODEL_DIR);
    }

    /**
     * Word2Vec Model을 로드한다.
     * @return WordVector RDD
     */
    public JavaPairRDD<String,WordVector> load(){
        DataFrame df = new DataFrameReader(SparkJDBC.getSQLContext()).json(Spark.HDFS_HOST+WORD2VEC_MODEL_DIR);
        return df.toJavaRDD().map(new Function<Row, WordVector>() {
            @Override
            public WordVector call(Row v1) throws Exception {
                String word = v1.getString(1);
                List<Double> vector = v1.getStruct(0).getList(1);
                return new WordVector(word,vector);
            }
        }).mapToPair(new PairFunction<WordVector, String, WordVector>() {
            @Override
            public Tuple2<String, WordVector> call(WordVector wordVector) throws Exception {
                return new Tuple2<>(wordVector.getWord(),wordVector);
            }
        });
    }
}
