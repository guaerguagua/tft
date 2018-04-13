package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.DbUtil;
import com.fr.data.utils.MgmUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;

public class DetailBalance extends AbstractTableData {

	private String[] columnNames = null;

	private int columnNum = 10;

	private int colNum = 0;

	private ArrayList valueList = null;

	private String tablePrefix=null;

	private String transCdTotal=null;

	private String checkList=null;

	public DetailBalance() {
		//
//		setDefaultParameters(new Parameter[] { new Parameter("trans_cd"),new Parameter("day") });

		tablePrefix="tbl_fcl_ck_acct_balance";
		checkList=" user_id,acct_no,current_balance/100 ";

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

		String userId =parameters[0].getValue().toString();
		String acctNo=parameters[1].getValue().toString();

		FRContext.getLogger().info("\nuserId: " + userId+
					"\nacctNo:"+acctNo+"\n");

		//get db conn  and talbe Name

        Connection conn=DbUtil.getActConnection();
		// create sql
		String tableName=tablePrefix;
		String sql = getSql(userId,acctNo,tableName);
		FRContext.getLogger().info("Query SQL of DetailBalance: \n" + sql+"\n");

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
					"Query SQL of DetailBalance: \n" + valueList.size()
							+ " rows selected");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public String getSql(String userId,String acctNo,String tableName){

		String condition="";

		if(!userId.equals("")){
			condition=condition+String.format(" and user_id='%s' ",userId);
		}
		if(!acctNo.equals("")){
			condition=condition+String.format(" and acct_no='%s' ",acctNo);
		}

		String sql = String.format("select %s from %s where 1=1 %s limit 100000;",
							checkList,tableName,condition);

		return  sql;
	}

	// release
	public void release() throws Exception {
		super.release();
		this.valueList = null;
	}
}