package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.DbUtil;
import com.fr.data.utils.MgmUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;

public class TotalInsData extends AbstractTableData {

	private String[] columnNames = null;

	private int columnNum = 10;

	private int colNum = 0;

	private ArrayList valueList = null;

	private String tablePrefix=null;

	private String checkList=null;

	public TotalInsData() {

		tablePrefix="tbl_fcl_ins_acct_balance_hist";
		checkList=" ins_mchnt_cd,settle_dt,begin_balance/100,debit_at,credit_at , current_balance/100 ";

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

		String insMchntCd = parameters[0].getValue().toString();
		String dateStr=parameters[1].getValue().toString();
		FRContext.getLogger().info("\ndateStr:"+dateStr+"\ninsMchntCd:"+insMchntCd+"\n");

		//get db conn  and talbe Name
//
//		String tablePostfix=MgmUtil.getPostfix(dateStr,tablePrefix);
//        Connection conn;
//		if(tablePostfix.length()==1){
//			conn=DbUtil.getActConnection();
//		}else {
//			conn=DbUtil.getHisConnection();
//		}
		Connection conn=DbUtil.getActConnection();
		// create sql
		String tableName=tablePrefix;
		String sql = getSql(insMchntCd,dateStr,tableName);
		FRContext.getLogger().info("Query SQL of TotalInsData: \n" + sql+"\n");

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
					"Query SQL of TotalInsData: \n" + valueList.size()
							+ " rows selected");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public String getSql(String insMchntCd,String dateStr,String tableName){

		String condition=new String();
		String sql =new String();
		String settleDt=dateStr.replace("-","");
		boolean isHis=false;

		if(insMchntCd.equals("")){
			condition="";
		}else {
			condition=condition+String.format(" and ins_mchnt_cd='%s' ",insMchntCd);
		}

		sql = String.format("select %s from %s where settle_dt='%s' %s limit 10000;",
								checkList,tableName,settleDt,condition);

		return  sql;
	}

	// release
	public void release() throws Exception {
		super.release();
		this.valueList = null;
	}
}