package com.fr.data.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class DbUtil {
    private static final String URLPREFIX ="jdbc:mysql://88.88.15.11:3306/";
    private static final String USERNAME ="test";
    private static final String PASSWORD ="test";

    private static final String ACTDB="tftactdb";
    private static final String HISDB="tfthisdb";
    private static final String TEST ="tfttest";
    private static final String MGMDB ="tftmgmdb";

    private static Connection getConnection(String tablename){
        String driverName = "com.mysql.jdbc.Driver";
        String url = URLPREFIX+tablename;
        String username = USERNAME;
        String password = PASSWORD;

        try {
            Class.forName(driverName);
            java.sql.Connection con = DriverManager.getConnection(url, username, password);
            return con;
        } catch (Exception var7) {
            var7.printStackTrace();
            return null;
        }
    }

    public static Connection getActConnection() {
        return getConnection(ACTDB);
    }

    public static Connection getHisConnection() {
        return getConnection(HISDB);
    }

    public static Connection getTestConnection() {
        return getConnection(TEST);
    }

    public static Connection getMgmConnection() {
        return getConnection(MGMDB);
    }
}
