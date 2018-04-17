package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.DbUtil;
import com.fr.data.utils.MgmUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;

public class DetailRequestData extends AbstractTableData {

	private String[] columnNames = null;

	private int columnNum = 10;

	private int colNum = 0;

	private ArrayList valueList = null;

	private String tablePrefix=null;

	private String transCdTotal=null;

	private String checkList=null;

	public DetailRequestData() {
		//
//		setDefaultParameters(new Parameter[] { new Parameter("trans_cd"),new Parameter("day") });

		tablePrefix="tbl_fcl_request";
		checkList="req_no,settle_dt,req_module,req_para,req_rst_desc,req_rst_msg,rec_crt_ts";


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

		String transCd =parameters[0].getValue().toString();
		String startDateStr=parameters[1].getValue().toString();
		String endDateStr=parameters[2].getValue().toString();
		String acctNo=parameters[3].getValue().toString();
		String bussNo=parameters[4].getValue().toString();
		String phoneNo=parameters[5].getValue().toString();
		FRContext.getLogger().info(String.format("\n transCd=[%s],startDateStr=[%s],endDateStr=[%s],acctNo=[%s],bussNo=[%s],phoneNo=[%s]",
				transCd,startDateStr,endDateStr,acctNo,bussNo,phoneNo));

		if(acctNo.equals("")&&!phoneNo.equals("")){
			acctNo=MgmUtil.fromPhoneNoGetAcctNo(phoneNo);
		}
		//get db conn  and talbe Name
		valueList = new ArrayList();
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
			FRContext.getLogger().info("Query SQL of "+dateStr+" DetailRequestData: \n" + sql+"\n");

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
					"Query SQL of DetailRequestData: \n" + valueList.size()
							+ " rows selected");

	}


	public String getSql(String transCd,String acctNo,String bussNo,String tableName){

		String condition="";
		String sqlAcct="";
		if(!acctNo.equals("")){
			String [] list_act=acctNo.split(",");

			if(list_act.length>0){

				sqlAcct=String.format(" and req_para like '%%%s%%' ",list_act[0]);
				for(int i=1;i<list_act.length;i++){
					sqlAcct=String.format("%s or req_para like '%%%s%%'",sqlAcct,list_act[i]);
				}
			}
		}


		if(!bussNo.equals("")){
			condition=condition+String.format(" and req_no='%s' ",bussNo);
		}
		String patton=new String();
		if(condition.equals("")&&sqlAcct.equals("")){
			patton="select %s from %s where 1=1 %s %s limit 10;";
		}else {
			patton="select %s from %s where 1=1 %s %s ;";
		}

		if(!transCd.equals("")){
			condition=condition+String.format(" and trans_cd in (%s) ",MgmUtil.addQuot(transCd));
		}

		String sql = String.format(patton, checkList,tableName,condition,sqlAcct);

		return  sql;
	}

	// release
	public void release() throws Exception {
		super.release();
		this.valueList = null;
	}
}