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

    public String getCurrNo(){
        String sql = "select curr_log_no from tbl_mgm_settle_dt;";

        Connection conn = DbUtil.getMgmConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            String currLogNo=rs.getObject(1).toString();
            // 释放数据库资源
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

    public int getDayOfYear(String dateStr){
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
}
