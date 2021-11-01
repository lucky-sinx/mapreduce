package com.wy.mapreduce.hivedemo;

import org.junit.Test;

import java.io.*;

public class LoadData {
    public static void main(String[] args) {
        HiveBase con = new HiveBase();
        String sql = null;

        // 建立Country 表
        sql = "CREATE TABLE IF NOT EXISTS "
                + " QueryCountry ( datetime String, Query String, "
                + " IsImplicitIntent String, Country String,PopularityScore Int)"
                + " ROW FORMAT DELIMITED"
                + " FIELDS TERMINATED BY '\t'"
                + " LINES TERMINATED BY '\n'"
                + "TBLPROPERTIES ('skip.header.line.count'='1')";
        con.execute(sql);

        //建立State表
        sql = "CREATE TABLE IF NOT EXISTS "
                + " QueryState ( datetime String, Query String, "
                + " IsImplicitIntent String, State String, Country String,PopularityScore Int)"
                + " ROW FORMAT DELIMITED"
                + " FIELDS TERMINATED BY '\t'"
                + " LINES TERMINATED BY '\n'"
                + "TBLPROPERTIES ('skip.header.line.count'='1')";
        con.execute(sql);

        //导入数据,读取文件目录列表，每个文件都进行一次load data
        String data_path = "";
        File file = new File("src/main/java/com/wy/mapreduce/hivedemo/datafilelist");//Text文件
        BufferedReader br = null;//构造一个BufferedReader类来读取文件
        try {
            br = new BufferedReader(new FileReader(file));
            data_path = null;
            while ((data_path = br.readLine()) != null) {//使用readLine方法，一次读一行
//                System.out.println(data_path);
                if (data_path.contains("Country")) { //这是一个country表文件
                    sql = String.format("load data local inpath '%s' overwrite into table querycountry", data_path);
                } else {//这是一个state表文件
                    sql = String.format("load data local inpath '%s' overwrite into table querystate", data_path);
                }
                con.execute(sql);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        con.close();
    }

    @Test
    public void test_readfile() {
        File file = new File("src/main/java/com/wy/mapreduce/hivedemo/datafilelist");//Text文件
        BufferedReader br = null;//构造一个BufferedReader类来读取文件
        try {
            br = new BufferedReader(new FileReader(file));
            String s = null;
            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行
                System.out.println(s);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


