package com.dgcse.entity;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/**
 * Created by leeyh on 2016. 9. 1..
 */
public class SamplePage implements Serializable {
    private int id;
    private String url;
    private long date;
    private String title;
    private String body;
    private int type;

    public SamplePage(Page page){
        this.id = page.getId();
        this.url = page.getUrl();
        this.date = page.getDate();
        this.title = page.getTitle();
        this.body = page.getBody();
        this.type = 0;
    }

    public SamplePage(Row row){
        this.body = row.getString(0);
        this.date = row.getLong(1);
        this.id = row.getInt(2);
        this.title = row.getString(3);
        this.type = row.getInt(4);
        this.url = row.getString(5);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public static StructType getStructType(){
        return new StructType(new StructField[]{
                new StructField("body", DataTypes.StringType,false,Metadata.empty()),
                new StructField("date",DataTypes.LongType,false,Metadata.empty()),
                new StructField("id", DataTypes.IntegerType,false, Metadata.empty()),
                new StructField("title",DataTypes.StringType,false,Metadata.empty()),
                new StructField("type", DataTypes.IntegerType,false, Metadata.empty()),
                new StructField("url",DataTypes.StringType,false, Metadata.empty())
        });
    }
}
