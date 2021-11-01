package com.wy.mapreduce.hivedemo;

import org.apache.hive.jdbc.HiveDriver;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveBase {
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private Connection con = null;
    private Statement stmt = null;


    public HiveBase() {
        try {
            System.out.println(HiveDriver.class.getName());
            Class.forName(HiveDriver.class.getName());
            con = DriverManager.getConnection(
                    "jdbc:hive2://192.168.10.102:10000/default", "wy", "");
            stmt = con.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public HiveBase(String url, String user, String password) {
        try {
            con = DriverManager.getConnection(url, user, password);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    /*用于执行具有返回结果的sql语句，例如create*/
    public ResultSet executeQuery(String sql) {
        ResultSet res = null;
        try {
            System.out.println("[Running]=> " + sql);
            res = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res;
    }

    /*用于执行没有返回结果的sql语句，例如select*/
    public void execute(String sql) {
        try {
            System.out.println("[Running]=> " + sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            if (con != null)
                con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
