package com.michael.zookeeper.distribute_server_study;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

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
public class DistributeClient {
    private static final String host ="192.168.68.163:2181,192.168.68.163:2182,192.168.68.163:2183";
    private static final int timeOut = 2000;
    private static String groupNode = "/servers";

    private ZooKeeper zkCli = null;

    //注册的服务信息存储的列表
    volatile List<String> servers = null;



    public static void main(String[] args) {
        //获取zk上的服务器列表
        DistributeClient distributeClient = new DistributeClient();
        try {
            //获取服务列表信息
            distributeClient.getOnlineServer();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //实现自己的业务逻辑
        distributeClient.handler();
    }


    /**
     * 获取在线的服务器节点
     */
    public void getOnlineServer() throws IOException {
        //服务注册端不需要实现watcher的监听
         zkCli = new ZooKeeper(host, timeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //从zookeeper中获取最新的服务节点信息
                //一旦节点发生变化，监听就会触发，然后获取一次服务节点
                updateServers();
            }
        });
        /**
         * 一进来就调用一次
         */
        updateServers();
    }

    /**
     * 从zk中获取获取子节点（注册的服务）
     */
    public void updateServers(){
        try {
            //存储服务节点的真实信息
            List<String> serverList = new ArrayList<>();
            //获取服务节点下的子节点
            List<String> childrenList = zkCli.getChildren(groupNode,true);
            //获取所有节点下的data——服务器的主机及端口信息
            for(String serverChild :childrenList){
                byte[] data = zkCli.getData(groupNode+"/"+serverChild,false,null);
                //将获取到的节点下的服务器信息保存到list中
                String dataStr = "";
                try {
                     dataStr = new String(data,"utf-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                if(! dataStr.equals("")){
                    serverList.add(dataStr);
                    System.out.println("当前在线的服务节点有" + dataStr);
                }
            }
            //将注册的服务信息列表放到全局变量环境中
            servers = serverList;
            System.out.println("服务节点更新***********华丽的分割线******");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * 业务逻辑实现
     */
    public void handler(){
        System.out.println("客户端处理自己的业务逻辑*****");
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
