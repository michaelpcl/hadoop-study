package com.michael.operation;

import com.michael.utils.HbaseUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseOperation {
    //链接
    private  Connection connection;

    public HbaseOperation(){
        this.connection = HbaseUtils.generatorConnection();
    }

    /**
     * 根据表名查询全部记录
     * @param tableName
     */
    public void queryAllData(String tableName){
        Admin admin = null;
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        TableName table = TableName.valueOf(tableName);
        try {
            admin.enableTables("acw:history_hbase_foss");
        } catch (IOException e) {
            e.printStackTrace();
        }
        Scan scan=new Scan();
        ResultScanner rs = null;
        try {
            rs = connection.getTable(table).getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(rs != null) {
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));
                }
            }
        }
    }


    /**
     * 根据rowKey查询
     */
    public void queryDataByRowKey(String tableName,String rowKey){
        // 根据rowkey查询
        Get get = new Get(rowKey.getBytes());
        TableName table = TableName.valueOf(tableName);
        Result r = null;
        try {
            r = connection.getTable(table).get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("获得到rowkey:" + new String(r.getRow()));
        if(r != null) {
            for (KeyValue keyValue : r.raw()) {
                KeyValue temp = keyValue;
                System.out.println("*******" + keyValue.getKeyString());
                System.out.println("列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));
            }
        }
    }


    /**
     * 单条件查询
     */
    public void queryDataByCondition(String tableName,String columnName){
        TableName table = TableName.valueOf(tableName);
        Filter filter = new SingleColumnValueFilter(Bytes
                .toBytes("column1"), null, CompareFilter.CompareOp.EQUAL, Bytes
                .toBytes(columnName)); // 当列column1的值为columnName时进行查询
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner rs = null;
        try {
            rs = connection.getTable(table).getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(rs != null) {
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));
                }
            }
        }
    }


    /**
     * 多条件查询
     */
    public void queryDataByCondition2(){

    }



}
