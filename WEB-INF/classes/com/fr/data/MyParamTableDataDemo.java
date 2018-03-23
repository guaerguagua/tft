package com.fr.data;

import com.fr.base.FRContext;
import com.fr.data.utils.DbUtil;
import com.fr.file.DatasourceManager;

import java.sql.*;
import java.util.ArrayList;

/**
 * �������ĳ������ݼ�Demo
 *
 * @author fanruan
 */
public class MyParamTableDataDemo extends AbstractTableData {
    /**
     * �������飬����������ݼ���������
     */
    private String[] columnNames;
    /**
     * ����������ݼ���������
     */
    private int columnNum = 10;
    /**
     * �����ѯ���ʵ��������
     */
    private int colNum = 0;
    /**
     * �����ѯ�õ���ֵ
     */
    private ArrayList valueList = null;

    /**
     * ���캯���������ṹ���ñ���10�������У�����Ϊcolumn#0��column#1��������������column#9
     */
    public MyParamTableDataDemo() {
        columnNames = new String[columnNum];
        for (int i = 0; i < columnNum; i++) {
            columnNames[i] = "column#" + String.valueOf(i);
        }
    }

    /**
     * ʵ�������ĸ�����
     *
     * @return columnNum
     */
    @Override
    public int getColumnCount() {
        return columnNum;
    }

    @Override
    public String getColumnName(int columnIndex) {
        return columnNames[columnIndex];
    }

    @Override
    public int getRowCount() {
        init();
        return valueList.size();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        init();
        if (columnIndex >= colNum) {
            return null;
        }
        return ((Object[]) valueList.get(rowIndex))[columnIndex];
    }

    /**
     * ׼������
     */
    private void init() {
        // ȷ��ֻ��ִ��һ��
        if (valueList != null) {
            return;
        }
        // ����õ������ݿ����
        String begin = parameters[0].getValue().toString();
        String end   = parameters[1].getValue().toString();
        FRContext.getLogger().info("begin:"+begin);
        FRContext.getLogger().info("end:"+end);


        // ����SQL���,����ӡ����
        String sql = "select * from s_resource";
        FRContext.getLogger().info("Query SQL of Param" +
                "TableDataDemo: \n" + sql);
        // ����õ��Ľ����
        valueList = new ArrayList();
        // ���濪ʼ�������ݿ����ӣ����ողŵ�SQL�����в�ѯ
        //com.fr.data.impl.Connection conn = DatasourceManager.getInstance().getConnection("FRDemo");

        try {
            //Connection con = conn.createConnection();
            Connection con = DbUtil.getTestConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            // ��ü�¼����ϸ��Ϣ��Ȼ����������
            ResultSetMetaData rsmd = rs.getMetaData();
            colNum = rsmd.getColumnCount();
            // �ö��󱣴�����
            Object[] objArray = null;
            while (rs.next()) {
                objArray = new Object[colNum];
                for (int i = 0; i < colNum; i++) {
                    objArray[i] = rs.getObject(i + 1);
                }
                // ��valueList�м�����һ������
                valueList.add(objArray);
            }
            // �ͷ����ݿ���Դ
            rs.close();
            stmt.close();
            con.close();
            // ��ӡһ��ȡ��������������

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * �ͷ�һЩ��Դ����Ϊ���ܻ����ظ����ã��������ͷ�valueList�����ϴβ�ѯ�Ľ���ͷŵ�
     *
     * @throws Exception e
     */
    @Override
    public void release() throws Exception {
        super.release();
        this.valueList = null;
    }
}