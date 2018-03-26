package com.fr.data.utils;

import com.fr.base.FRContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MgmUtil {

    //ri qie biao
    public static String getCurrNo(){
        String sql = "select curr_log_no from tbl_mgm_settle_dt;";

        Connection conn = DbUtil.getMgmConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            String currLogNo=rs.getObject(1).toString();
            //release resouce
            rs.close();
            stmt.close();
            conn.close();
            FRContext.getLogger().info(
                    "curr_log_no= "+currLogNo );
            return currLogNo;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //his year hao
    public static int getHisLogNo(String dateStr){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//		String dateStr="2018-03-28";
        Date date=new Date();
        try {
            date = sdf.parse(dateStr);
            Calendar calendar=Calendar.getInstance();
            calendar.setTime(date);

            return (calendar.get(Calendar.YEAR))%3+1;
        }catch (ParseException e){
            e.printStackTrace();
        }
        return 0;

    }

    public static int getDayOfYear(String dateStr){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//		String dateStr="2018-03-28";
        Date date=new Date();
        try {
            date = sdf.parse(dateStr);
            Calendar calendar=Calendar.getInstance();
            calendar.setTime(date);

            return calendar.get(Calendar.DAY_OF_YEAR);
        }catch (ParseException e){
            e.printStackTrace();
        }
        return 0;
    }

    public static String getTableNamePostfix(String tableNamePrefix,String yestodayStr,String currLogNo){

        String yesLogNo;

        if(currLogNo.equals("1")){
            yesLogNo="2";
        }else {
            yesLogNo="1";
        }
        String tableName=tableNamePrefix+yesLogNo;

        String sql= "select count(*) from "+tableName+";";

        Connection conn = DbUtil.getActConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            String countNumStr=rs.getObject(1).toString();
            //release resouce
            rs.close();
            stmt.close();
            conn.close();
            FRContext.getLogger().info(
                    "count(*) from " + tableName+ " is "+countNumStr );
            if(countNumStr.equals("0")){
                return String.format("%d_%03d",getHisLogNo(yestodayStr),getDayOfYear(yestodayStr));
            }else {
                return yesLogNo;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static Date getYestoday(){

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) - 1);
        return calendar.getTime();
    }
}
