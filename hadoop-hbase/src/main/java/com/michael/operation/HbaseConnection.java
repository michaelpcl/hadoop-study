package com.michael.operation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseConnection {
    Admin admin=null;
    private static Configuration cfg=null;

    public  static  void ScanHbase() throws  Exception {

        cfg = HBaseConfiguration.create();
        cfg.set("hbase.master", "192.168.10.229:60000");
        cfg.set("hbase.zookeeper.quorum", "192.168.10.229:2181,192.168.10.230:2181,192.168.10.231:2181");
        // cfg.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(cfg);
        //HBASE_CONFIG.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181") ;
        // HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.10.229,192.168.10.230,192.168.10.231");

        //HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
       /* cfg = new HBaseConfiguration(HBASE_CONFIG);
            System.out.println("000000000000000");
        admin = new HBaseAdmin(cfg);
        admin.enableTables("acw:history_hbase_foss");
            System.out.println("1111111111111");*/
        Admin admin1 = conn.getAdmin();


        TableName table = TableName.valueOf("acw:history_hbase_foss");
        admin1.enableTables("acw:history_hbase_foss");

        Scan scan=new Scan();

        FilterList filterList=new FilterList();
        PrefixFilter prefixFilter=new PrefixFilter(Bytes.toBytes("03"));
        /**
         * 这个可以添加不同filter
         */

        filterList.addFilter(prefixFilter);
        scan.setFilter(filterList);

        ResultScanner rs = conn.getTable(table).getScanner(scan);
        for (Result r : rs) {
            System.out.println("获得到rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
                System.out.println("列：" + new String(keyValue.getFamily())
                        + "====值:" + new String(keyValue.getValue()));
            }
        }
    }


    public static void main(String[] args) {
        try {
            ScanHbase();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
