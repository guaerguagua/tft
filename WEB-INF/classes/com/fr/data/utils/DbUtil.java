package com.fr.data.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class DbUtil {
    private static final String URLFORMAT ="jdbc:mysql://%s:%s/%s";

    private static final String ACTDB="tftactdb";
    private static final String ACTDBIP="88.88.15.17";
    private static final String ACTDBPORT="3306";
    private static final String ACTDBUSER="test";
    private static final String ACTDBPASS="test";

    private static final String HISDB="tfthisdb";
    private static final String HISDBIP="88.88.15.17";
    private static final String HISDBPORT="3306";
    private static final String HISDBUSER="test";
    private static final String HISDBPASS="test";

    private static final String TEST ="tfttest";
    private static final String TESTIP ="88.88.15.17";
    private static final String TESTPORT ="3306";
    private static final String TESTUSER ="test";
    private static final String TESTPASS ="test";

    private static final String MGMDB ="tftmgmdb";
    private static final String MGMDBIP ="88.88.15.17";
    private static final String MGMDBPORT ="3306";
    private static final String MGMDBUSER ="test";
    private static final String MGMDBPASS ="test";

    private static Connection getConnection(String dbname,String username,String password,String ip,String port){
        String driverName = "com.mysql.jdbc.Driver";
        String url = String.format(URLFORMAT,ip,port,dbname);
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
        return getConnection(ACTDB,ACTDBUSER,ACTDBPASS,ACTDBIP,ACTDBPORT);
    }

    public static Connection getHisConnection() {
        return getConnection(HISDB,HISDBUSER,HISDBPASS,HISDBIP,HISDBPORT);
    }

    public static Connection getTestConnection() {
        return getConnection(TEST,TESTUSER,TESTPASS,TESTIP,TESTPORT);
    }

    public static Connection getMgmConnection() {
        return getConnection(MGMDB,MGMDBUSER,MGMDBPASS,MGMDBIP,MGMDBPORT);
    }
}
