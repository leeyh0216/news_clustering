package com.dgcse.entity;

import java.io.Serializable;

/**
 * Created by leeyh on 2016. 8. 28..
 */
public class Word implements Serializable {
    private int pageId;
    private String word;
    private double tf;

    public Word(int pageId,String word){
        this.pageId = pageId;
        this.word = word;
        this.tf = 0;
    }

    @Override
    public int hashCode() {
        //Page ID와 Word가 동일하다면 같은 Word Entity이다.
        String identity = pageId+"/"+word;
        return identity.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(this.hashCode()==obj.hashCode())
            return true;
        else
            return false;
    }

    public int getPageId() {
        return pageId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public double getTf() {
        return tf;
    }

    public void setTf(double tf) {
        this.tf = tf;
    }


}
