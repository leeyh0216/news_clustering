package com.dgcse.entity;

import java.io.Serializable;

/**
 * Created by leeyh on 2016. 8. 28..
 */
public class InnerWord implements Serializable{
    private String word;
    private double cnt;

    public InnerWord(String word){
        this.word = word;
        this.cnt = 0;
    }

    @Override
    public int hashCode() {
        //단어의 Hashcode를 Object의 Hashcode로 사용한다.
        return word.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(this.hashCode()==obj.hashCode())
            return true;
        else
            return false;
    }

    public void increaseCnt(){
        cnt++;
    }

    public String getWord(){
        return word;
    }

    public double getCnt(){
        return cnt;
    }
}
