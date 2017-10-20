package com.michael.operation;

import com.michael.utils.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HbaseOperation {

    public static Configuration configuration;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.10.228,192.168.10.229,192.168.10.230,192.168.10.231,192.168.10.232");
        configuration.set("hbase.master", "192.168.10.228:60000");
    }

    /**
     * 根据表名称查询
     * @param tableName
     */
    public static void queryAllData(String tableName){
        HTablePool pool = new HTablePool(configuration, 1000);
        //HTable table = (HTable) pool.getTable(tableName);
        try {
            ResultScanner rs = pool.getTable(tableName).getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        queryAllData("bse:t_bas_own_truck");
    }


}
