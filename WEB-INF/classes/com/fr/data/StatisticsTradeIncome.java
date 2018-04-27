package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.Check;
import com.fr.data.utils.DbUtil;
import com.fr.data.utils.MgmUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 *
 * @author fanruan
 */
public class StatisticsTradeIncome extends AbstractTableData {
    /**
     *
     */
    private String[] columnNames;
    /**
     *
     */
    private int columnNum = 6;
    /**
     *
     */
    private int colNum = 6;
    /**
     *
     */
    private ArrayList valueList = null;

    /**
     *
     */
    public StatisticsTradeIncome() {
        columnNames = new String[columnNum];
        columnNames[0]="tradeType";
        columnNames[1]="charChan";
        columnNames[2]="passageWay";
        columnNames[3]="sum_trade_count";
        columnNames[4]="sum_trade_amount";
        columnNames[5]="sum_user_count";
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



    /**
     *
     */
    private void init() {
        //
        if (valueList != null) {
            return;
        }
        //
        String settleDt = parameters[0].getValue().toString();
        String transCd = parameters[1].getValue().toString();
        FRContext.getLogger().info("settleDt:"+settleDt);
        valueList = new ArrayList();

        Check c = new Check();
        c.checkValue(Check.TRANS_CD_ID,transCd)
                .checkValue(Check.SETTLE_DT_ID,settleDt);

        if(!c.getRes()) {
            FRContext.getLogger().info("输入不合法，没有通过检验");
            return ;
        }

        String tablePrefix = "tbl_fcl_ck_acct_dtl";
        String suffix = MgmUtil.getPostfix(settleDt,tablePrefix);
        String tableName = tablePrefix+suffix;
        FRContext.getLogger().info("suffix:"+suffix);
        String sql;
        if(transCd.equals("")) {
            sql = String.format("select trans_cd ,charChan, passageWay,count(*),cast(sum(trans_at)/100 as decimal(20,2)),count(distinct(acct_no)) from %s where trans_cd ='1401' group by charChan,passageWay \n" +
                    "UNION ALL\n" +
                    "select trans_cd ,charChan,passageWay,count(*),cast(sum(trans_at)/100 as decimal(20,2)),count(distinct(acct_no)) from %s where trans_cd ='1402'group by charChan,passageWay \n" +
                    "union ALL\n" +
                    "select trans_cd ,charChan, passageWay,count(*),cast(sum(trans_at)/100 as decimal(20,2)),count(distinct(acct_no)) from %s where trans_cd ='1408'group by charChan,passageWay;", tableName, tableName,tableName);
        }else{
            sql = String.format("select trans_cd ,charChan,passageWay,count(*),cast(sum(trans_at)/100 as decimal(20,2)),count(distinct(acct_no)) from %s where trans_cd='%s'group by charChan,passageWay;", tableName, transCd);
        }
        FRContext.getLogger().info("Query SQL of Param\n"  + sql);

        try {
            Connection con=null;
            if(suffix.length() == 1) {
                con = DbUtil.getActConnection();
            }else{
                con = DbUtil.getHisConnection();
            }
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            FRContext.getLogger().info("get result of sql");
            while(rs.next()){
                Object[] objects = new Object[colNum];
                for(int i=0;i<colNum;i++){
                    objects[i]=rs.getObject(i+1);
                }
                valueList.add(objects);
            }
            rs.close();
            stmt.close();
            con.close();
            //

        } catch (Exception e) {
            e.printStackTrace();
        }

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
