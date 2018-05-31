package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.Check;
import com.fr.data.utils.DbUtil;
import com.fr.data.utils.MgmUtil;
import com.fr.web.core.A.B;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;

public class DetailActInData extends AbstractTableData {

	private String[] columnNames = null;

	private int columnNum = 10;

	private int colNum = 0;

	private ArrayList valueList = null;

	private String tablePrefix=null;

	private String transCdTotal=null;

	private String checkList=null;

	public DetailActInData() {
		//
//		setDefaultParameters(new Parameter[] { new Parameter("trans_cd"),new Parameter("day") });

		tablePrefix="tbl_fcl_ck_acct_dtl";
		checkList="settle_dt, buss_no,acct_no,trans_cd,trans_at/100 ,rec_crt_ts,charChan,passageWay,current_balance/100,current_balance/100-trans_at/100";
		transCdTotal="1401,1402,1408";

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

		String transCd 		=	parameters[0].getValue().toString();
		String startDateStr	=	parameters[1].getValue().toString();
		String endDateStr	=	parameters[2].getValue().toString();
		String acctNo		=	parameters[3].getValue().toString();
		String bussNo		=	parameters[4].getValue().toString();
		String phoneNo		=	parameters[5].getValue().toString();
		FRContext.getLogger().info(String.format("\n transCd=[%s],startDateStr=[%s],endDateStr=[%s],acctNo=[%s],bussNo=[%s],phoneNo=[%s]",
				transCd,startDateStr,endDateStr,acctNo,bussNo,phoneNo));

		valueList = new ArrayList();
		Check check=new Check();
		check.checkValue(Check.BUSS_NO_ID,bussNo).checkValue(Check.ACCT_NO_ID,acctNo).checkValue(Check.PHONE_NO_ID,phoneNo).checkValue(Check.TRANS_CD_ID,transCd);
		if(!check.getRes()){
			FRContext.getLogger().info(String.format(" param wrong!!!!!!!!"));
			return;
		}

		if(acctNo.equals("")&&!phoneNo.equals("")){
			acctNo=MgmUtil.fromPhoneNoGetAcctNo(phoneNo);
		}

		if(acctNo.length()+bussNo.length()==0){
			return;
		}
		//get db conn  and talbe Name

		String dateStr=startDateStr;

		while(!MgmUtil.date1after2(dateStr,endDateStr)){

			Connection conn;

			String tablePostfix=MgmUtil.getPostfix(dateStr,tablePrefix);

			if(tablePostfix.length()==1){
				conn=DbUtil.getActConnection();
			}else {
				conn=DbUtil.getHisConnection();
			}

			// create sql
			String tableName=tablePrefix+tablePostfix;
			String sql = getSql(transCd,acctNo,bussNo,tableName);
			FRContext.getLogger().info("Query SQL of "+dateStr+" DetailActInData: \n" + sql+"\n");

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
			} catch (Exception e) {
				e.printStackTrace();
			}

			dateStr=MgmUtil.getDateStrDiff(dateStr,1);
		}
		FRContext.getLogger().info(
					"Query SQL of DetailActInData: \n" + valueList.size()
							+ " rows selected");

	}


	public String getSql(String transCd,String acctNo,String bussNo,String tableName){

		String condition="";

		if(!acctNo.equals("")){
			condition=condition+String.format(" and acct_no in (%s) ",MgmUtil.addQuot(acctNo));
		}
		if(!bussNo.equals("")){
			condition=condition+String.format(" and buss_no='%s' ",bussNo);
		}

		String patton=new String();
		if(condition.equals("")){
			patton="select %s from %s where 1=1 %s limit 0;";
		}else {
			patton="select %s from %s where 1=1 %s  order by rec_crt_ts ;";
		}

		if(transCd.equals("")){
			condition=condition+String.format(" and trans_cd in (%s) ",MgmUtil.addQuot(transCdTotal));;
		}else {
			condition=condition+String.format(" and trans_cd in (%s) ",MgmUtil.addQuot(transCd));
		}

		String sql = String.format(patton, checkList,tableName,condition);

		return  sql;
	}

	// release
	public void release() throws Exception {
		super.release();
		this.valueList = null;
	}
}