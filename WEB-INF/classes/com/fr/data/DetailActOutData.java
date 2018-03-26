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

	private String tablePrefix=null;

	private String transCdTotal=null;

	private String checkList=null;

	public DetailActOutData() {
		//
//		setDefaultParameters(new Parameter[] { new Parameter("trans_cd"),new Parameter("day") });

		tablePrefix="tbl_fcl_ck_acct_dtl";
		checkList=" settle_dt,buss_no,acct_no,trans_cd,trans_at,ins_mchnt_cd ,rec_crt_ts ";
		transCdTotal="1403,1407,1409";

		columnNames = checkList.replaceAll(" ","").split(",");
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
		// get parame
		String transCd = parameters[0].getValue().toString();
		String dateStr=parameters[1].getValue().toString();
		String acctNo=parameters[2].getValue().toString();
		FRContext.getLogger().info("\ntrans_cd: " + transCd+"\ndateStr:"+dateStr+"\nacctNo"+acctNo+"\n");

		//get db conn  and talbe Name
		String tablePostfix=MgmUtil.getPostfix(dateStr,tablePrefix);

		Connection conn;
		if(tablePostfix.length()==1){
			conn=DbUtil.getActConnection();
		}else {
			conn=DbUtil.getHisConnection();
		}
		// create sql
		String tableName=tablePrefix+tablePostfix;
		String sql = getSql(transCd,acctNo,tableName);
		FRContext.getLogger().info("Query SQL of DetailActOutData: \n" + sql+"\n");

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
					"Query SQL of DetailActOutData: \n" + valueList.size()
							+ " rows selected");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public String getSql(String transCd,String acctNo,String tableName){

		String condition=null;
		String sql =null;

		if(transCd.equals("")){
			condition="";
		}else {
			condition=String.format(" and trans_cd in (%s) ",transCd);
		}
		if (!acctNo.equals("")){
			condition=condition+String.format(" and acct_no=%s ",acctNo);
		}

		sql = String.format("select %s  from %s where trans_cd in( %s ) %s ;",
								checkList,tableName,transCdTotal,condition);

		return  sql;
	}

	// release
	public void release() throws Exception {
		super.release();
		this.valueList = null;
	}
}