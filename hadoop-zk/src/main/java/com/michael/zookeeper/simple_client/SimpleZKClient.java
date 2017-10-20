package com.michael.zookeeper.simple_client;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * Project name hadoop-study
 * <p>
 * Package name com.michael.zookeeper
 * <p>
 * Description:
 * <p>
 * Created by 326007
 * <p>
 * Created date 2017/7/19
 */
public class SimpleZKClient {
    private static final String host ="192.168.68.163:2181,192.168.68.163:2182,192.168.68.163:2183";
    private static final int timeOut = 2000;


    public static void main(String args[]){
        try {
            /**
             * 参数说明
             * host
             * timeout
             * 监听器
             */
            ZooKeeper zkCli = new ZooKeeper(host, timeOut, new Watcher() {
                /**
                 * zk服务器集群监听到某个指定数据节点发生事件后，会通知监听的注册者（客户端）
                 * 然后客户端会调用process方法，将接受到的event事件作为process的参数
                 * @param watchedEvent
                 */
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println("节点："+ watchedEvent.getPath()+ "发生了事件" + watchedEvent.getType());
                }
            });

            try {
                //获取跟节点下的子节点
                List<String> childrenList= zkCli.getChildren("/",true);
                for(String path :childrenList){
                    System.out.println(path);
                }
                System.out.println(childrenList.size());
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
