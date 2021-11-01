package com.wy.mapreduce.bigdatahomework;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountryTable implements WritableComparable<CountryTable> {
    private String date; //日期
    private String query; //查询关键词
    private boolean isImplicitIntent; //是否包含covid
    private String country; //国家
    private int popularityScore; //流行度

    public CountryTable() {
    }

    public String getDate() {
        return date;
    }

    public String getQuery() {
        return query;
    }

    public boolean isImplicitIntent() {
        return isImplicitIntent;
    }

    public String getCountry() {
        return country;
    }

    public int getPopularityScore() {
        return popularityScore;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setImplicitIntent(boolean implicitIntent) {
        isImplicitIntent = implicitIntent;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setPopularityScore(int popularityScore) {
        this.popularityScore = popularityScore;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(date);
        out.writeUTF(query);
        out.writeBoolean(isImplicitIntent);
        out.writeUTF(country);
        out.writeInt(popularityScore);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
//        this.id = in.readUTF();
//        this.pid = in.readUTF();
//        this.amount = in.readInt();
//        this.pname = in.readUTF();
//        this.flag = in.readUTF();
        this.date = in.readUTF();
        this.query = in.readUTF();
        this.isImplicitIntent = in.readBoolean();
        this.country = in.readUTF();
        this.popularityScore = in.readInt();
    }

    @Override
    public String toString() {
        return "TableBean{" +
                "Date='" + date + '\'' +
                ", Query='" + query + '\'' +
                ", IsImplicitIntent=" + isImplicitIntent +
                ", Country='" + country + '\'' +
                ", PopularityScore=" + popularityScore +
                '}';
    }

    @Override
    public int compareTo(CountryTable o) {
        if (this.popularityScore > o.popularityScore) {
            return -1;
        } else if (this.popularityScore < o.popularityScore) {
            return 1;
        } else {
            return 0;
        }
    }
}
