package com.dgcse.processor;

import com.dgcse.entity.InnerWord;
import com.dgcse.entity.Page;
import com.dgcse.entity.Word;
import com.google.common.collect.Lists;
import com.twitter.penguin.korean.KoreanPosJava;
import com.twitter.penguin.korean.KoreanTokenJava;
import com.twitter.penguin.korean.TwitterKoreanProcessorJava;
import com.twitter.penguin.korean.tokenizer.KoreanTokenizer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.List;

/**
 * Created by leeyh on 2016. 8. 28..
 */
public class WordExtractor implements Serializable{

    private JavaRDD<Page> pageRDD;

    public WordExtractor(JavaRDD<Page> pageRDD){
        this.pageRDD = pageRDD;
    }

    public WordExtractor(){

    }

    /**
     * pageRDD를 WordRDD로 변환한다.
     * @return WordRDD
     */
    public JavaRDD<Word> pageRddToWordRdd(){
        return pageRDD.flatMap(new FlatMapFunction<Page, Word>() {
            @Override
            public Iterable<Word> call(Page page) throws Exception {
                return extractWordFromParagraph(page.getId(),page.getBody());
            }
        });
    }

    public List<String> extractWordList(String body){
        List<KoreanTokenJava> list = stemmingLine(body);
        List<String> wordList = Lists.newArrayList();

         if(list.size()==0)
            return Lists.newArrayList();

        //실제 사용할 단어만 걸러낸다.
        for (KoreanTokenJava koreanTokenJava : list) {
            //고유명사와 명사만을 걸러낸다.
            if ((koreanTokenJava.getPos().equals(KoreanPosJava.ProperNoun) || koreanTokenJava.getPos().equals(KoreanPosJava.Noun)) && koreanTokenJava.getLength() > 1){
                String word = koreanTokenJava.getText();
                wordList.add(word);
            }
        }

        return wordList;
    }
    /**
     * Page의 Body에서 Word를 추출한다.
     * @param pageId Page ID
     * @param body Page Body
     * @return WordList(List)
     */
    private List<Word> extractWordFromParagraph(int pageId,String body){
        Hashtable<String,InnerWord> innerWordHashTable = new Hashtable<>();
        double allCnt = 0;
        List<KoreanTokenJava> list = stemmingLine(body);

        if(list.size()==0)
            return Lists.newArrayList();

        //실제 사용할 단어만 걸러낸다.
        for (KoreanTokenJava koreanTokenJava : list) {
            //고유명사와 명사만을 걸러낸다.
            if ((koreanTokenJava.getPos().equals(KoreanPosJava.ProperNoun) || koreanTokenJava.getPos().equals(KoreanPosJava.Noun)) && koreanTokenJava.getLength() > 1){
                String word = koreanTokenJava.getText();

                allCnt++;
                if(!innerWordHashTable.containsKey(word))
                    innerWordHashTable.put(word,new InnerWord(word));

                innerWordHashTable.get(word).increaseCnt();
            }
        }

        List<Word> wordList = Lists.newArrayList();

        for (InnerWord innerWord : innerWordHashTable.values()) {
            Word w = new Word(pageId,innerWord.getWord());
            w.setTf(innerWord.getCnt()/allCnt);
            wordList.add(w);
        }

        return wordList;
    }

    /**
     * 문장을 Stemming한다.
     * @param line Stemming할 문장
     * @return Stemming된 문장(오류 시 empty list)
     */
    public static List<KoreanTokenJava> stemmingLine(String line){
        try {
            CharSequence normalized = TwitterKoreanProcessorJava.normalize(line);
            Seq<KoreanTokenizer.KoreanToken> tokens = TwitterKoreanProcessorJava.tokenize(normalized);
            Seq<KoreanTokenizer.KoreanToken> stemmed = TwitterKoreanProcessorJava.stem(tokens);
            return TwitterKoreanProcessorJava.tokensToJavaKoreanTokenList(stemmed);
        }
        catch(Exception e){
            return Lists.newArrayList();
        }
    }

}
