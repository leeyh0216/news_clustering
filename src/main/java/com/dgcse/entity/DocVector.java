package com.dgcse.entity;

import com.dgcse.processor.Word2VecProcessor;

import java.io.Serializable;
import java.util.List;

/**
 * Created by leeyh on 2016. 9. 1..
 */
public class DocVector implements Serializable {
    private int id;
    private List<Double> vector;
    private int center;

    public DocVector(int id,List<Double> vector){
        this.id = id;
        this.vector= vector;
    }

    public void setCenter(int center){
        this.center = center;
    }

    public int getCenter(){
        return center;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public List<Double> getVector() {
        return vector;
    }

    public void setVector(List<Double> vector) {
        this.vector = vector;
    }

    public static double getSimilarity(DocVector d1,DocVector d2){
        double similarity = 0;
        List<Double> d1Vector = d1.getVector();
        List<Double> d2Vector = d2.getVector();

        for(int i = 0;i< Word2VecProcessor.VECTOR_SIZE;i++)
            similarity+=Math.pow(d1Vector.get(i)-d2Vector.get(i),2);

        return Math.sqrt(similarity);

    }
}
