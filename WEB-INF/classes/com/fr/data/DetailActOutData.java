package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.DbUtil;
import com.fr.data.utils.MgmUtil;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DetailActOutData extends AbstractTableData {

	private String[] columnNames = null;

	private int columnNum = 10;

	private int colNum = 0;

	private ArrayList valueList = null;


	public DetailActOutData() {
		//
//		setDefaultParameters(new Parameter[] { new Parameter("trans_cd"),new Parameter("day") });


		columnNames = new String[]{"settle_dt","buss_no","acct_no","trans_cd","trans_at","ins_mchnt_cd","acct_name" };
		columnNum=columnNames.length;
	}

	public int getColumnCount() {
		return columnNum;
	}

	public String getColumnName(int columnIndex) {
		return columnNames[columnIndex];
	}

	public int getRowCount() {
		init();
		return valueList.size();
	}

	public Object getValueAt(int rowIndex, int columnIndex) {
		init();
		if (columnIndex >= colNum) {
			return null;
		}
		return ((Object[]) valueList.get(rowIndex))[columnIndex];
	}


	public void init() {

		if (valueList != null) {
			return;
		}

		String transCd = parameters[0].getValue().toString();
		String dateStr=parameters[1].getValue().toString();
		FRContext.getLogger().info("trans_cd: " + transCd+"dateStr:"+dateStr+"\n");

		//get db conn  and talbe Name
		boolean isHis=false;
		String tableNamePos=new String();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String today=sdf.format(new Date());
        Connection conn ;
		if(today.equals(dateStr)){
		    String currLogNo=MgmUtil.getCurrNo();
			tableNamePos=currLogNo;
            isHis=false;
            conn= DbUtil.getActConnection();
		}else {
            isHis=true;
            tableNamePos= String.format("%d_%03d",
                            MgmUtil.getHisLogNo(dateStr),MgmUtil.getDayOfYear(dateStr));
            conn=DbUtil.getHisConnection();
        }

		// create sql
		String sql = getSql(transCd,tableNamePos);
		FRContext.getLogger().info("Query SQL of ParamTableDataDemo: \n" + sql+"\n");

		valueList = new ArrayList();

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			// get total col Num
			ResultSetMetaData rsmd = rs.getMetaData();
			colNum = rsmd.getColumnCount();
			// save Object
			Object[] objArray = null;
			while (rs.next()) {
				objArray = new Object[colNum];
				for (int i = 0; i < colNum; i++) {
					objArray[i] = rs.getObject(i + 1);
				}
				//add line
				valueList.add(objArray);
			}

			rs.close();
			stmt.close();
			conn.close();

			FRContext.getLogger().info(
					"Query SQL of DetailTableActData: \n" + valueList.size()
							+ " rows selected");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public String getSql(String transCd,String tableNamePos){

		String condition=new String();
		String sql =new String();

		if(transCd.equals("")){
			condition="";
		}else {
			condition="and trans_cd="+transCd;
		}
//		sql = "select settle_dt, buss_no,acct_no,trans_cd,trans_at from "+ tableName + " where 1=1 "+condition +";";

		String balanceTable=new String();
		if(tableNamePos.length()==1){
			balanceTable="tbl_fcl_ins_acct_balance";
		}else {
			balanceTable=String.format("tbl_fcl_ins_acct_balance_hist%s",tableNamePos);
		}
		sql=String.format("select a.settle_dt,a.buss_no,a.acct_no,a.trans_cd," +
				"a.trans_at,c.ins_mchnt_cd,c.acct_name  " +
				"from  tbl_fcl_ck_acct_dtl%s a  left join tbl_fcl_ins_acct_dtl%s b" +
				" on a.buss_no=b.buss_no left join %s c on b.acct_no = c.acct_no " +
				"where 1=1 %s",tableNamePos,tableNamePos,balanceTable,condition);
		return  sql;
	}



	// 获取数据库连接 driverName和 url 可以换成您需要的
	public Connection getConnection() {
		String driverName = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://88.88.15.11:3306/tfttest";
		String username = "test";
		String password = "test";
		Connection con = null;
		try {
			Class.forName(driverName);
			con = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return con;
	}


	// 释放一些资源，因为可能会有重复调用，所以需释放valueList，将上次查询的结果释放掉
	public void release() throws Exception {
		super.release();
		this.valueList = null;
	}
}