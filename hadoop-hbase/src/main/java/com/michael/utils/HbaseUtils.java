package com.michael.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;

public class HbaseUtils {

    private static Configuration configuration;
    private static Connection connection;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.10.228,192.168.10.229,192.168.10.230,192.168.10.231,192.168.10.232");
        configuration.set("hbase.master", "192.168.10.228:60000");
    }

    public static Connection generatorConnection(){
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }



}
