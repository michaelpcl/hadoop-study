package com.michael;

import com.michael.operation.HbaseOperation;

public class HbaseTest {
    public static void main(String[] args) {
        HbaseOperation hbaseOperation = new HbaseOperation();
        //hbaseOperation.queryAllData("acw:history_hbase_foss");
        //hbaseOperation.queryDataByRowKey("acw:history_hbase_foss","03450948-7-W011304060706");
        hbaseOperation.queryDataByCondition("acw:history_hbase_foss","in_time");


    }
}
