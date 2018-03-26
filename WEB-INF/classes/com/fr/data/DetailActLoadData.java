package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.DbUtil;
import com.fr.data.utils.MgmUtil;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DetailActLoadData extends AbstractTableData {

	private String[] columnNames = null;

	private int columnNum = 10;

	private int colNum = 0;

	private ArrayList valueList = null;

	private String tablePrefix=null;

	private String transCdTotal=null;

	public DetailActLoadData() {
		//
//		setDefaultParameters(new Parameter[] { new Parameter("trans_cd"),new Parameter("day") });

		tablePrefix="tbl_fcl_ck_acct_dtl";
		columnNames = new String[]{"settle_dt", "buss_no","acct_no","trans_cd","trans_at","rec_crt_ts"};
		transCdTotal="1410,1411,1412";
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
		String acctNo=parameters[2].getValue().toString();
		FRContext.getLogger().info("\ntrans_cd: " + transCd+"\ndateStr:"+dateStr+"\nacctNo"+acctNo+"\n");

		//get db conn  and talbe Name
		boolean isHis=false;
		String tableNo=new String();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String today=sdf.format(new Date());
		String yestoday=sdf.format(MgmUtil.getYestoday());

		if(today.equals(dateStr)){
		    String currLogNo=MgmUtil.getCurrNo();
			tableNo=currLogNo;
            isHis=false;
		}else if(yestoday.equals(dateStr)){
			tableNo=MgmUtil.getTableNamePostfix(tablePrefix,yestoday,MgmUtil.getCurrNo());
		} else {
            isHis=true;
			tableNo= String.format("%d_%03d",
                            MgmUtil.getHisLogNo(dateStr),MgmUtil.getDayOfYear(dateStr));
        }
        Connection conn;
		if(tableNo.length()==1){
			conn=DbUtil.getActConnection();
		}else {
			conn=DbUtil.getHisConnection();
		}
		// create sql
		String tableName=tablePrefix+tableNo;
		String sql = getSql(transCd,acctNo,tableName);
		FRContext.getLogger().info("Query SQL of DetailActLoadData: \n" + sql+"\n");

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
					"Query SQL of DetailActLoadData: \n" + valueList.size()
							+ " rows selected");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public String getSql(String transCd,String acctNo,String tableName){

		String condition=new String();
		String sql =new String();
		boolean isHis=false;

		if(transCd.equals("")){
			condition="";
		}else {
			condition=String.format(" and trans_cd in (%s) ",transCd);
		}
		if (!acctNo.equals("")){
			condition=condition+String.format(" and acct_no=%s ",acctNo);
		}

		sql = String.format("select settle_dt, buss_no,acct_no,trans_cd,trans_at ,rec_crt_ts " +
				" from %s where trans_cd in( %s ) %s ;",tableName,transCdTotal,condition);

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