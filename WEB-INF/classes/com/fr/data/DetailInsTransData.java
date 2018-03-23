package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.DbUtil;
import com.fr.data.utils.MgmUtil;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DetailInsTransData extends AbstractTableData {

	private String[] columnNames = null;

	private int columnNum = 10;

	private int colNum = 0;

	private ArrayList valueList = null;


	public DetailInsTransData() {
		//
//		setDefaultParameters(new Parameter[] { new Parameter("trans_cd"),new Parameter("day") });


		columnNames = new String[]{"ins_mchnt_cd","acct_name", "trans_cd","buss_no","trans_at"  };
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
		String insMchntCd=parameters[2].getValue().toString();
		FRContext.getLogger().info("\ntrans_cd: " + transCd+"\ndateStr:"+dateStr+"\ninsMchntCd"+insMchntCd+"\n");

		//get db conn  and talbe Name
		boolean isHis=false;
		String tableNamePostfix;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String today=sdf.format(new Date());
        Connection conn ;
		if(today.equals(dateStr)){
			tableNamePostfix=MgmUtil.getCurrNo();
            isHis=false;
            conn= DbUtil.getActConnection();
		}else {
            isHis=true;
			tableNamePostfix= String.format("%d_%03d",
                            MgmUtil.getHisLogNo(dateStr),MgmUtil.getDayOfYear(dateStr));
            conn=DbUtil.getHisConnection();
        }

		// create sql
		String sql = getSql(transCd,tableNamePostfix,insMchntCd,isHis);
		FRContext.getLogger().info("Query SQL of DetailInsTransData: \n" + sql+"\n");

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
					"Query SQL of DetailInsTransData: \n" + valueList.size()
							+ " rows selected");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public String getSql(String transCd,String tableNamePostfix,String insMchntCd,boolean isHis){

		String condition;

		if(transCd.equals("")){
			condition="";
		}else {
			condition=String.format(" and trans_cd=%s ",transCd);
		}
		if(!insMchntCd.equals("")){
			condition=condition+String.format(" and ins_mchnt_cd=%s ",insMchntCd);
		}
		String balanceTable;
		if(isHis){
			balanceTable=String.format("tbl_fcl_ins_acct_balance_hist%s",tableNamePostfix);
		}else {
			balanceTable="tbl_fcl_ins_acct_balance";
		}
		return String.format("select b.ins_mchnt_cd,b.acct_name, a.trans_cd,a.buss_no,a.trans_at " +
				" from tbl_fcl_ins_acct_dtl%s a left join %s b" +
				" on a.acct_no=b.acct_no where 1=1 %s ;",tableNamePostfix,balanceTable,condition);

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