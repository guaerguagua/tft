package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.Check;
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

	private String tablePrefix=null;

	private String checkList=null;

	public DetailInsTransData() {
		//
//		setDefaultParameters(new Parameter[] { new Parameter("trans_cd"),new Parameter("day") });

		tablePrefix="tbl_fcl_ins_acct_dtl";
		checkList=" settle_dt,buss_no,acct_no,trans_cd,trans_at/100,rec_crt_ts ";


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
		String transCd		= parameters[0].getValue().toString();
		String dateStr		= parameters[1].getValue().toString();
		String insAcctNo	= parameters[2].getValue().toString();
		String bussNo		= parameters[3].getValue().toString();
		FRContext.getLogger().info("\ntrans_cd: " + transCd+
				"\ndateStr:"+dateStr+"\nacctNo"+insAcctNo+"\nbussNo"+bussNo+"\n");
		valueList = new ArrayList();
		Check check=new Check();
		check.checkValue(Check.BUSS_NO_ID,bussNo).checkValue(Check.INS_ACCT_NO_ID,insAcctNo).checkValue(Check.TRANS_CD_ID,transCd);
		if(!check.getRes()){
			FRContext.getLogger().info(String.format(" param wrong!!!!!!!!"));
			return;
		}

		if(bussNo.equals("")){
			return;
		}

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
		String sql = getSql(transCd,insAcctNo,bussNo,tableName);
		FRContext.getLogger().info("Query SQL of DetailInsTransData: \n" + sql+"\n");

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


	public String getSql(String transCd,String insAcctNo,String bussNo,String tableName){

		String condition="";

		if(!insAcctNo.equals("")){
			condition=condition+String.format(" and acct_no in (%s) ",MgmUtil.addQuot(insAcctNo));
		}
		if(!bussNo.equals("")){
			condition=condition+String.format(" and buss_no='%s' ",bussNo);
		}
		if(!transCd.equals("")){
			condition=condition+String.format(" and trans_cd in (%s) ",MgmUtil.addQuot(transCd));
		}

		String sql = String.format("select %s  from %s where 1=1  %s  order by rec_crt_ts ;",
				checkList,tableName,condition);

		return  sql;
	}

	// release
	public void release() throws Exception {
		super.release();
		this.valueList = null;
	}
}