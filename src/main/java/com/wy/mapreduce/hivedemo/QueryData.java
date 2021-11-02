package com.wy.mapreduce.hivedemo;

import java.sql.ResultSet;
import java.sql.SQLException;

public class QueryData {

    public static void main(String[] args) {
        HiveBase con = new HiveBase();
        String sql = null;
        ResultSet res = null;

        //查询state表记录数
//        sql = "select count(*) from querystate";
//        res = con.executeQuery(sql);
//        printResultSet(res);
//        //查询country表记录数
//        sql = "select count(*) from querycountry";
//        res = con.executeQuery(sql);
//        printResultSet(res);
        //
        sql = "select * from querycountry where country='China' and IsImplicitIntent='True' limit 100";
        res = con.executeQuery(sql);
        printResultSet(res, 5);
        con.close();
    }

    public static void printResultSet(ResultSet res, int length) {
        while (true) {
            try {
                if (!res.next()) break;
                String one_line = "";
                for (int i = 1; i < length; i++) {
//                    System.out.print(res.getString(i) + "\t");
                    one_line += res.getString(i) + "\t";
                }
                one_line += res.getString(length) + "\t";
                System.out.println(one_line);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
