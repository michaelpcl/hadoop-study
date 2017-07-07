package com.michael.namenode_client;

import com.michael.namenode.DemoNameNodePro;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Project name hadoop-study
 * <p>
 * Package name com.michael.namenode_client
 * <p>
 * Description:
 * <p>
 * Created by 326007
 * <p>
 * Created date 2017/7/7
 */
public class DemoNameNodeClient {
    /**

     * 在使用这个之前，需要先启动服务端

     */

    public static void main(String[] args) throws IOException {

        //通过这种方式获取到一个远程调用对象
        DemoNameNodePro nameNode = RPC.getProxy(
                //客户端也需要定义这个接口类，而且包名（路径）需要和服务端保持一致
                DemoNameNodePro.class, //接口类，双方的协议类
                1L,//版本号
                new InetSocketAddress("10.224.197.145",10000),
                new Configuration()
        );

        //调用服务端的接口，看看返回的结果
        System.out.println("客户端开始调用");

        String metaData = nameNode.getMetaData("/hdfs/test.txt");
        System.out.println(metaData);

    }
}
