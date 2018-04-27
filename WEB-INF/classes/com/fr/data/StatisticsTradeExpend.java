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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 *
 * @author fanruan
 */
public class StatisticsTradeExpend extends AbstractTableData {
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
    public StatisticsTradeExpend() {
        columnNames = new String[columnNum];
        columnNames[0]="tradeType";
        columnNames[1]="ins_merchant_no";
        columnNames[2]="ins_merchant_name";
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
        FRContext.getLogger().info("transCd:"+transCd);
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
        String sql = null;
        if(transCd.equals("")) {
            sql = String.format("select trans_cd ,ins_mchnt_cd, ins_mchnt_cd,count(*),cast(sum(trans_at)/100 as decimal(20,2)),count(distinct(acct_no)) from %s where trans_cd = '1403' group by ins_mchnt_cd\n" +
                    "union ALL\n" +
                    "select trans_cd ,ins_mchnt_cd, ins_mchnt_cd,count(*),cast(sum(trans_at)/100 as decimal(20,2)),count(distinct(acct_no)) from %s where trans_cd = '1407' group by ins_mchnt_cd\n" +
                    "union ALL\n" +
                    "select trans_cd ,ins_mchnt_cd, ins_mchnt_cd,count(*),cast(sum(trans_at)/100 as decimal(20,2)),count(distinct(acct_no)) from %s where trans_cd = '1409' group by ins_mchnt_cd;", tableName, tableName,tableName);

        }else{
            sql = String.format("select trans_cd ,ins_mchnt_cd, ins_mchnt_cd,count(*),cast(sum(trans_at)/100 as decimal(20,2)),count(distinct(acct_no)) from %s where trans_cd='%s' group by ins_mchnt_cd;", tableName, transCd);
        }
        //
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
