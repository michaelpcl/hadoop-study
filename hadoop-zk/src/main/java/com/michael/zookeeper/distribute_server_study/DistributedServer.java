package com.michael.zookeeper.distribute_server_study;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * Project name hadoop-study
 * <p>
 * Package name com.michael.zookeeper.distribute_server_study
 * <p>
 * Description:
 * <p>
 * Created by 326007
 * <p>
 * Created date 2017/8/1
 */
public class DistributedServer {
    private static final String host ="192.168.68.163:2181,192.168.68.163:2182,192.168.68.163:2183";
    private static final int timeOut = 2000;
    private static String groupNode = "/servers";

    public static void main(String args[]){
        //String IP = "10.224.197.10";
        //String port = "14816";

        //调用registeZK 向zk中注册服务器信息
        DistributedServer distributedServer = new DistributedServer();
        try {
            distributedServer.registeZK(args[0],args[1]);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //实现业务逻辑
        distributedServer.handle(args[0],args[1]);
    }

    /**
     * 业务逻辑处理
     */
    public void handle(String IP,String port){
        System.out.println("服务器开始处理事情了" + IP + "***********" + port);
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * 创建一个zk客户端，定义一个监听器逻辑
     * 这里服务端不需要实现watcher的监听逻辑
     * 在ZK中创建一个znode节点
     * @param  IP, port
     *
     * 注册的服务的ip，以及启动端口
     */
    public void registeZK(String IP,String port) throws Exception {
        //服务注册端不需要实现watcher的监听
        ZooKeeper zkCli = new ZooKeeper(host, timeOut, null);

        /**
         * 在服务组下 创建节点
         * 节点下的内容是 ip + port
         * createMode 是临时节点的形式
         */
        String path = zkCli.create(groupNode + "/server01", (IP+"-"+port).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("服务器在zk中注册了一个子节点" + path);
    }
}
