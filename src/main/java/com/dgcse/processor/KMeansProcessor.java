package com.dgcse.processor;

import com.dgcse.common.LogInstance;
import com.dgcse.entity.DocVector;
import com.dgcse.entity.Page;
import com.dgcse.entity.SamplePage;
import com.dgcse.entity.WordVector;
import com.dgcse.spark.Spark;
import com.dgcse.spark.SparkJDBC;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created by leeyh on 2016. 9. 1..
 */
public class KMeansProcessor implements Serializable {
    private static final String SAMPLE_PAGE_TABLE = "sample_page";
    private static final String PROCESSED_PAGE_TABLE = "classificated_page";
    private JavaRDD<SamplePage> samplePageRDD;
    private JavaRDD<DocVector> docVectorRDD;
    private static final int K_SIZE = 1000;
    private static final int ITERATION_STEP = 12;
    private boolean isFirst = true;

    public KMeansProcessor(){

    }

    private JavaRDD<DocVector> samplePageRddToDocVectorRdd(JavaRDD<SamplePage> samplePageRDD){
        JavaPairRDD<String,WordVector> wordVectorRDD = new Word2VecProcessor().load();

        //각 문서에서 Word를 추출한 단어를 기준으로 그룹핑한다.
        JavaPairRDD<String,Iterable<WordVector>> groupedWordRDD = samplePageRDD.repartition(Spark.CNT_ALL_CORE).flatMap(new FlatMapFunction<SamplePage, WordVector>() {
            @Override
            public Iterable<WordVector> call(SamplePage samplePage) throws Exception {
                String body = samplePage.getBody();
                List<String> wordList = new WordExtractor().extractWordList(body);

                List<WordVector> wordVectorList = Lists.newArrayList();

                for (String s : wordList)
                    wordVectorList.add(new WordVector(s,samplePage.getId()));

                return wordVectorList;
            }
        }).groupBy(new Function<WordVector, String>() {
            @Override
            public String call(WordVector v1) throws Exception {
                return v1.getWord();
            }
        });

        //Word2Vec으로 추출한 WordVector와 현재 문서들에서 추출된 WordVector(vector이 설정되어있지 않은)을 join 후
        //DocVector로 만든 후 리턴.
        return groupedWordRDD.leftOuterJoin(wordVectorRDD).repartition(Spark.CNT_ALL_CORE).flatMap(new FlatMapFunction<Tuple2<String,Tuple2<Iterable<WordVector>,Optional<WordVector>>>, WordVector>() {
            @Override
            public Iterable<WordVector> call(Tuple2<String, Tuple2<Iterable<WordVector>, Optional<WordVector>>> stringTuple2Tuple2) throws Exception {
                //WordVector가 존재하는 경우
                if(stringTuple2Tuple2._2()._2().isPresent()){
                    for (WordVector wordVector : stringTuple2Tuple2._2()._1())
                        wordVector.setVector(stringTuple2Tuple2._2()._2().get().getVector());
                    return stringTuple2Tuple2._2()._1();
                }
                else
                    return Lists.newArrayList();

            }
        }).groupBy(new Function<WordVector, Integer>() {
            @Override
            public Integer call(WordVector v1) throws Exception {
                return v1.getPageId();
            }
        }).repartition(Spark.CNT_ALL_CORE).map(new Function<Tuple2<Integer,Iterable<WordVector>>, DocVector>() {
            @Override
            public DocVector call(Tuple2<Integer, Iterable<WordVector>> v1) throws Exception {
                int id = v1._1();

                double[] arr = new double[Word2VecProcessor.VECTOR_SIZE];

                double cnt = 0;
                for (WordVector wordVector : v1._2()) {
                    List<Double> vector = wordVector.getVector();
                    for(int i = 0; i< Word2VecProcessor.VECTOR_SIZE; i++)
                        arr[i]+=vector.get(i);
                    cnt++;
                }

                for(int i = 0;i<Word2VecProcessor.VECTOR_SIZE;i++)
                    arr[i]/=cnt;

                List<Double> vectorList = Lists.newArrayList();
                for(int i = 0;i<arr.length;i++)
                    vectorList.add(arr[i]);

                return new DocVector(id, vectorList);
            }
        });
    }

    public void init(){
        samplePageRDD = loadSampleData();
        samplePageRDD.cache();
        LogInstance.getLogger().debug("Sample Page Count : "+samplePageRDD.count());
        docVectorRDD = samplePageRddToDocVectorRdd(samplePageRDD);
        docVectorRDD.cache();
        LogInstance.getLogger().debug("DocVector Count : "+docVectorRDD.count());
    }

    public void start(){
        for(int i = 0;i<ITERATION_STEP;i++){
            LogInstance.getLogger().debug("KMeans Step : "+i);
            if(isFirst){
                runInFirst();
                isFirst = false;
            }
            else
                run();
        }

        List<Tuple2<Integer,Iterable<DocVector>>> clustered = docVectorRDD.mapToPair(new PairFunction<DocVector, Integer, DocVector>() {
            @Override
            public Tuple2<Integer, DocVector> call(DocVector docVector) throws Exception {
                return new Tuple2<Integer, DocVector>(docVector.getCenter(),docVector);
            }
        }).groupByKey().collect();

        for (Tuple2<Integer, Iterable<DocVector>> integerIterableTuple2 : clustered) {
            LogInstance.getLogger().debug("center : "+integerIterableTuple2._1());
            StringBuilder stb = new StringBuilder();

            for (DocVector docVector : integerIterableTuple2._2()) {
                stb = stb.append(docVector.getId()+" ,");
            }

            LogInstance.getLogger().debug(stb.toString());
        }

    }

    private List<DocVector> reCalcCenter(JavaRDD<DocVector> docVectorRDD){
        return docVectorRDD.mapToPair(new PairFunction<DocVector, Integer, DocVector>() {
            @Override
            public Tuple2<Integer, DocVector> call(DocVector docVector) throws Exception {
                return new Tuple2<Integer, DocVector>(docVector.getCenter(),docVector);
            }
        }).groupByKey().repartition(Spark.CNT_ALL_CORE).map(new Function<Tuple2<Integer,Iterable<DocVector>>, DocVector>() {
            @Override
            public DocVector call(Tuple2<Integer, Iterable<DocVector>> v1) throws Exception {
                double[] center = new double[Word2VecProcessor.VECTOR_SIZE];
                List<Double> tmpList = Lists.newArrayList();
                double allCnt = 0;

                //중심점을 구한다.
                for (DocVector docVector : v1._2()) {
                    allCnt++;
                    for(int i = 0;i<Word2VecProcessor.VECTOR_SIZE;i++)
                        center[i]+=docVector.getVector().get(i);
                }
                for(int i = 0;i<Word2VecProcessor.VECTOR_SIZE;i++) {
                    center[i] /= allCnt;
                    tmpList.add(center[i]);
                }

                //중심점과 가장 가까운 DocVector를 찾는다.
                DocVector centerVector = null;
                DocVector tmpCenter = new DocVector(0,tmpList);
                double maxSimilarity = Double.MIN_VALUE;

                for (DocVector docVector : v1._2())
                    if(maxSimilarity<DocVector.getSimilarity(tmpCenter,docVector))
                        centerVector = docVector;
                return centerVector;
            }
        }).filter(new Function<DocVector, Boolean>() {
            @Override
            public Boolean call(DocVector v1) throws Exception {
                if(v1==null)
                    return false;
                else
                    return true;
            }
        }).collect();
    }
    private void run(){
        final List<DocVector> centerDocVector = reCalcCenter(docVectorRDD);
        LogInstance.getLogger().debug("Re calced center list size : "+centerDocVector.size());
        JavaRDD<DocVector> tmpDocVector = docVectorRDD.repartition(Spark.CNT_ALL_CORE).map(new Function<DocVector, DocVector>() {
            @Override
            public DocVector call(DocVector v1) throws Exception {
                v1.setCenter(getSimilarId(centerDocVector,v1));
                return v1;
            }
        });

        docVectorRDD.unpersist();
        docVectorRDD = tmpDocVector;
        docVectorRDD.cache();
    }

    private void runInFirst(){
        final List<DocVector> sampleDocVector = docVectorRDD.takeSample(true,K_SIZE);
        LogInstance.getLogger().debug("Sample Doc Vector List size : "+sampleDocVector.size());
        JavaRDD<DocVector> tmpDocVector = docVectorRDD.repartition(Spark.CNT_ALL_CORE).map(new Function<DocVector, DocVector>() {
            @Override
            public DocVector call(DocVector v1) throws Exception {
                v1.setCenter(getSimilarId(sampleDocVector,v1));
                return v1;
            }
        });

        docVectorRDD.unpersist();
        docVectorRDD = tmpDocVector;
        docVectorRDD.cache();
    }

    private int getSimilarId(List<DocVector> centerDocVector,DocVector docVector){
        int id= -1;
        double similarity =Double.MAX_VALUE;
        for(DocVector center : centerDocVector){
            double currentSimilarity = DocVector.getSimilarity(center,docVector);
            if(currentSimilarity<similarity) {
                id = center.getId();
                similarity = currentSimilarity;
            }
        }
        return id;
    }

    /**
     * Clustering 할 Sample Data를 로드한다.
     * @return Clustering Sample Data RDD
     */
    private JavaRDD<SamplePage> loadSampleData(){
        return samplePageRDD = SparkJDBC.readTable(SAMPLE_PAGE_TABLE,SamplePage.getStructType()).map(new Function<Row, SamplePage>() {
            @Override
            public SamplePage call(Row v1) throws Exception {
                return new SamplePage(v1);
            }
        });
    }

    /**
     * 분류할 샘플 데이터를 생성한다.
     */
    public static void createSampleData(){
        //Page 데이터는 Comparable이며 순서는 date에 대한 asc
        JavaRDD<Page> pageRDD = SparkJDBC.readTable(Page.TABLE_NAME, Page.getStructType()).map(new Function<Row, Page>() {
            @Override
            public Page call(Row v1) throws Exception {
                return Page.rowToPage(v1);
            }
        });

        List<Page> orderedPageList = pageRDD.takeOrdered(5000);

        //새로운 번호를 인덱싱한다.
        int idx = 1;
        for (Page page : orderedPageList)
            page.setId(idx++);

        JavaRDD<SamplePage> samplePageRDD = Spark.getJavaSparkContext().parallelize(orderedPageList).map(new Function<Page, SamplePage>() {
            @Override
            public SamplePage call(Page v1) throws Exception {
                return new SamplePage(v1);
            }
        });
        SparkJDBC.getSQLContext().createDataFrame(samplePageRDD,SamplePage.class).write().mode(SaveMode.Overwrite).jdbc(SparkJDBC.DB_URL,SAMPLE_PAGE_TABLE,SparkJDBC.SQL_PROPERTIES);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        samplePageRDD.unpersist();
        docVectorRDD.unpersist();
    }

}
