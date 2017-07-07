package com.michael.namenode;

/**
 * Project name hadoop-study
 * <p>
 * Package name com.michael.namenode
 * <p>
 * Description:
 * <p>
 * Created by 326007
 * <p>
 * Created date 2017/7/7
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

import java.io.IOException;


/**
 * 将业务功能发布为RPC服务
 */
public class ServicePublisher {

    public static void main(String[] args) throws IOException {
        //获取一个创建RPC服务的builder
        Builder builder = new RPC.Builder(new Configuration());
        /**
         * 通过builder与业务功能建立联系
         * 设置服务端的服务实现的接口，该接口也是通讯双方遵循的协议
         */
        builder.setProtocol(DemoNameNodePro.class);
        //设置提供业务功能的具体实例对象
        builder.setInstance(new DemoNameNodeProImpl());

        //设置服务进程绑定的地址信息
        builder.setBindAddress("10.224.197.145");
        builder.setPort(10000);

        //用builder来创建一个服务——其实就是一个socket 服务
        Server server = builder.build();
        //启动服务，提供远程调用
        server.start();
        System.out.println("服务器已启动");
    }
}
