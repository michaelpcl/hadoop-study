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
        configuration = new Configuration();
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
