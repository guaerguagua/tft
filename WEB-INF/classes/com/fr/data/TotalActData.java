package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.DbUtil;
import com.fr.data.utils.MgmUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;

public class TotalActData extends AbstractTableData {

	private String[] columnNames = null;

	private int columnNum = 10;

	private int colNum = 0;

	private ArrayList valueList = null;

	private String tablePrefix=null;

	private String checkList=null;

	public TotalActData() {

		tablePrefix="tbl_fcl_ck_acct_balance_hist";
		checkList=" acct_no,settle_dt,begin_balance/100,debit_at,credit_at , current_balance/100 ";

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

		String acctNo = parameters[0].getValue().toString();
		String dateStr=parameters[1].getValue().toString();
		FRContext.getLogger().info("\ndateStr:"+dateStr+"\nacctNo"+acctNo+"\n");

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
		String sql = getSql(acctNo,tableName);
		FRContext.getLogger().info("Query SQL of TotalActData: \n" + sql+"\n");

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
					"Query SQL of TotalActData: \n" + valueList.size()
							+ " rows selected");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public String getSql(String acctNo,String tableName){

		String condition=new String();
		String sql =new String();
		boolean isHis=false;

		if(acctNo.equals("")){
			condition= " limit 100000";
		}else {
			condition=condition+String.format(" and acct_no='%s' ",acctNo);
		}

		sql = String.format("select %s from %s where 1=1 %s ;",
								checkList,tableName,condition);

		return  sql;
	}

	// release
	public void release() throws Exception {
		super.release();
		this.valueList = null;
	}
}