package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.Check;
import com.fr.data.utils.DbUtil;
import com.fr.data.utils.MgmUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 *
 *
 * @author fanruan
 */
public class StatisticsUserAct extends AbstractTableData {
    /**
     *
     */
    private String[] columnNames;
    /**
     *
     */
    private int columnNum = 4;
    /**
     *
     */
    private int colNum = 0;
    /**
     *
     */
    private ArrayList valueList = null;

    /**
     *
     */
    public StatisticsUserAct() {
        columnNames = new String[columnNum];
        columnNames[0]="begin_acct_count";
        columnNames[1]="sum_begin_balance";
        columnNames[2]="end_acct_count";
        columnNames[3]="sum_end_balance";
    }

    /**
     *
     *
     * @return columnNum
     */
    @Override
    public int getColumnCount() {
        return columnNum;
    }

    @Override
    public String getColumnName(int columnIndex) {
        return columnNames[columnIndex];
    }

    @Override
    public int getRowCount() {
        init();
        return valueList.size();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        init();
        if (columnIndex >= colNum) {
            return null;
        }
        return ((Object[]) valueList.get(rowIndex))[columnIndex];
    }
    private String getCurrentDate(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new  java.util.Date());
    }

    private boolean checkInput(String begin,String end){
        if(!MgmUtil.checkNumStr(begin,8)){
            return false;
        }
        if(!MgmUtil.checkNumStr(end,8)){
            return false;
        }
        return true;
    }

    /**
     *
     */
    private void init() {
        //
        if (valueList != null) {
            return;
        }
        //
        String begin = parameters[0].getValue().toString();
        String end   = parameters[1].getValue().toString();
        FRContext.getLogger().info("begin:"+begin);
        FRContext.getLogger().info("end:"+end);
        valueList = new ArrayList();

        Check c = new Check();
        c.checkValue(Check.SETTLE_DT_ID,begin)
                .checkValue(Check.SETTLE_DT_ID,end);

        if(!c.getRes()) {
            FRContext.getLogger().info("输入不合法，没有通过检验");
            return ;
        }

        String tablePrefix = "tbl_fcl_ck_acct_balance_hist";
        String beginTableName = tablePrefix+String.format("%d_%03d",
                MgmUtil.getHisLogNo(begin),MgmUtil.getDayOfYear(begin));
        String endTableName = tablePrefix+String.format("%d_%03d",
                MgmUtil.getHisLogNo(end),MgmUtil.getDayOfYear(end));

        Object[] objects = new Object[4];
        colNum=4;
        String sql = String.format("select count(*) as begin_acct_count,cast(sum(begin_balance)/100 as decimal(20,2)) as sum_begin_balance from %s;",beginTableName);
        //
        FRContext.getLogger().info("Query SQL of Param\n"  + sql);

        try {
            Connection con = DbUtil.getHisConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            FRContext.getLogger().info("get result of sql");
            rs.next();
            objects[0]=rs.getObject("begin_acct_count");
            FRContext.getLogger().info("begin_acct_count:"+rs.getInt("begin_acct_count")+"\n");
            objects[1]=rs.getObject("sum_begin_balance");
            FRContext.getLogger().info("sum_begin_balance:"+rs.getBigDecimal("sum_begin_balance")+"\n");
            //

//            objects[0]=rs.getObject(1);
//            FRContext.getLogger().info("begin_acct_count:"+rs.getInt(1)+"\n");
//            objects[1]=rs.getObject(2);
//            FRContext.getLogger().info("sum_begin_balance:"+rs.getBigDecimal(2)+"\n");
            //
            rs.close();
            stmt.close();
            con.close();
            //

        } catch (Exception e) {
            e.printStackTrace();
        }
        String sql2 = String.format("select count(*) as end_acct_count,cast(sum(current_balance)/100 as decimal(20,2)) as sum_end_balance from %s;",endTableName);
        //
        FRContext.getLogger().info("Query SQL of Param\n"  + sql2);

        try {
            Connection con = DbUtil.getHisConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql2);
            FRContext.getLogger().info("get result of sql2");
            rs.next();
            objects[2]=rs.getObject("end_acct_count");
            objects[3]=rs.getObject("sum_end_balance");
            FRContext.getLogger().info("end_acct_count:"+rs.getInt("end_acct_count")+"\n");
            FRContext.getLogger().info("sum_end_balance:"+rs.getBigDecimal("sum_end_balance")+"\n");
            //

            //
            rs.close();
            stmt.close();
            con.close();
            //

        } catch (Exception e) {
            e.printStackTrace();
        }

        valueList.add(objects);
    }

    /**
     *
     *
     * @throws Exception e
     */
    @Override
    public void release() throws Exception {
        super.release();
        this.valueList = null;
    }
}
