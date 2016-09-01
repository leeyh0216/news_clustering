package com.dgcse.entity;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/**
 * Created by leeyh on 2016. 8. 27..
 */
public class Page implements Serializable,Comparable<Page>{
    public static final String TABLE_NAME = "page";

    private int id;
    private String url;
    private long date;
    private String title;
    private String body;

    public Page(int id,String url, String title, String body,long date) {
        this.id = id;
        this.url = url;
        this.date = date;
        this.title = title;
        this.body = body;
    }

    public int getId(){
        return id;
    }

    public void setId(int id){ this.id = id;}

    public String getUrl() {
        return url;
    }

    public long getDate() {
        return date;
    }

    public String getTitle() {
        return title;
    }

    public String getBody() {
        return body;
    }

    public static StructType getStructType(){
        return new StructType(new StructField[]{
                new StructField("id",DataTypes.IntegerType,false,Metadata.empty()),
                new StructField("url",DataTypes.StringType,false, Metadata.empty()),
                new StructField("urlhash",DataTypes.StringType,false,Metadata.empty()),
                new StructField("date",DataTypes.LongType,false,Metadata.empty()),
                new StructField("title",DataTypes.StringType,false,Metadata.empty()),
                new StructField("body", DataTypes.StringType,false,Metadata.empty()),
                new StructField("paragraph", DataTypes.StringType,false,Metadata.empty())
        });
    }

    public static Page rowToPage(Row row){
        int id = row.getInt(0);
        String url = row.getString(1);
        String title = row.getString(4);
        String body = row.getString(5);
        long date = row.getLong(3);
        return new Page(id,url,title,body,date);
    }

    @Override
    public int compareTo(Page o) {
        if(this.getDate()<o.getDate())
            return -1;
        else if(this.getDate()==o.getDate())
            return 0;
        else
            return 1;
    }
}
