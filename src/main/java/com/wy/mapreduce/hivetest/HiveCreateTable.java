package com.wy.mapreduce.hivetest;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

import org.apache.hive.jdbc.HiveDriver;

public class HiveCreateTable {
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            System.out.println(HiveDriver.class.getName());
            Class.forName(HiveDriver.class.getName());
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection(
                "jdbc:hive2://192.168.10.102:10000/test", "wy", "");
        Statement stmt = con.createStatement();
        String tableName = "records";
        String sql = "show tables";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }
        sql = "load data local inpath \"/home/wy/桌面/data/2021/QueriesByState_2021-06-01_2021-06-30.tsv\" into table querystate";
        stmt.executeQuery(sql);
        // describe table
//        sql = "describe " + tableName;
//        System.out.println("Running: " + sql);
//        res = stmt.executeQuery(sql);
//        while (res.next()) {
//            System.out.println(res.getString(1) + "\t" + res.getString(2));
//        }
//        sql = "select * from word_count";
//        res = stmt.executeQuery(sql);
//        while (res.next()) {
//            System.out.println(res.getString(1));
//        }
        con.close();
    }
}


