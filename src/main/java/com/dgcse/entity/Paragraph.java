package com.dgcse.entity;

import java.io.Serializable;

/**
 * Created by leeyh on 2016. 8. 28..
 */
public class Paragraph implements Serializable {
    private int pageId;
    private String line;

    public Paragraph(int pageId, String line){
        this.pageId = pageId;
        this.line = line;
    }

    public int getPageId(){
        return pageId;
    }

    public String getLine(){
        return line;
    }
}
