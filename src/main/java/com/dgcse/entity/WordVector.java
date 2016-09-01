package com.dgcse.entity;

import java.io.Serializable;
import java.util.List;

/**
 * Created by leeyh on 2016. 9. 1..
 */
public class WordVector implements Serializable{
    private String word;
    private List<Double> vector;
    private int pageId;

    public WordVector(String word,List<Double> vector){
        this.word = word;
        this.vector = vector;
    }

    public WordVector(String word,int pageId){
        this.word = word;
        this.pageId = pageId;
    }

    public String getWord(){
        return word;
    }

    public void setWord(String word){
        this.word = word;
    }

    public List<Double> getVector(){
        return vector;
    }

    public void setVector(List<Double> vector){
        this.vector = vector;
    }

    public int getPageId(){
        return pageId;
    }

    public void setPageId(int pageId){
        this.pageId = pageId;
    }
}
