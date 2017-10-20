package com.michael.zookeeper.distribute_lock;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Project name hadoop-study
 * Package name com.michael.zookeeper.distribute_lock
 * Description:
 * 多个服务获取共享锁
 * Created by 326007
 * Created date 2017/8/2
 */
public class DemoServer {

    private static final String host ="192.168.68.163:2181,192.168.68.163:2182,192.168.68.163:2183";
    private static final int timeOut = 2000;

    private ZooKeeper zkCli = null;
    private boolean haveLock = false;
    private static String groupNode = "/locks";
    private String myNodePath ;
    private static String hostName ;


    public static void main(String[] args) {
        hostName = args[0];
        System.out.println(hostName+ "************** get the lock");
        //获取锁，访问共享资源，执行业务逻辑
        DemoServer demoServer = new DemoServer();
        try {
            demoServer.getLockAndDoSomething();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //主线程休眠
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取共享锁，执行业务逻辑
     */
    public void getLockAndDoSomething() throws IOException {
        //构造一个zk客户端，定义一个监听器
        zkCli = new ZooKeeper(host, timeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //判断事件是不是子节点发生了变化的事件
                if(event.getType() != Event.EventType.NodeChildrenChanged) {
                    return;
                }
                else{
                    /**
                     * 当共享资源节点下面的字节点发生变化的时候，需要重新执行一遍如下的流程
                     */
                    //用zk客户端获取锁权限
                    haveLock = getLock();
                    if (haveLock) {
                        System.out.println(hostName + " get the lock");
                        //拿到锁之后执行业务逻辑
                        doSomething();
                        //释放所
                        releaseLock();
                        //重新创建一个锁节点，并注册监听
                        registerLock();
                    }
                }
            }
        });

        //首先各自需要注册自己的锁节点
        registerLock();
        //休眠500ms防止获取不到
        try {
            Thread.sleep((long) (Math.random()*500 +500));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //用zk客户端获取锁权限
        haveLock = getLock();

        if(haveLock) {
            System.out.println(hostName + " get the lock");
            //拿到锁之后执行业务逻辑
            doSomething();

            //释放所
            releaseLock();

            //重新创建一个锁节点，并注册监听
            registerLock();
        }
    }


    /**
     * 获取锁权限
     * 如果自己创建的锁节点是所有锁节点中的最小的节点值，则获取到锁权限
     *
     */
    public boolean getLock(){
        try {
            //获取到子节点
            List<String> childList = zkCli.getChildren(groupNode,true);
            //如果子节点元素只有一个，则当前只有一个节点在线，有锁权限
            if(childList.size()==1){
                return true;
            }
            /**
             * 找到最小的节点
             * 排序
             */
            Collections.sort(childList);
            //获取最小的节点
            String minNode = childList.get(0);
            /**
             * 获取自己注册的节点
             * myNodePath的格式如下 = groupNode+lockxxxxx
             */
            if(minNode.equals(myNodePath.substring(groupNode.length()+1))){
                return true;
            }
            return false;

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 模拟访问共享资源，并执行业务逻辑
     */
    public void doSomething(){
        System.out.println("begin working***********");
        try {
            Thread.sleep((long) (Math.random()*500 +1000));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("work has complished***********");
    }


    /**
     * 释放所
     */
    public void releaseLock(){
        //删除自己的锁节点
        try {
            zkCli.delete(myNodePath,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * 注册子节点信息，用于竞争锁，每次释放完，需要重新注册
     */
    public void registerLock(){
        try {
            //向zk中注册一个自己的锁节点，并将锁节点记录到myNodePath中
            myNodePath = zkCli.create(groupNode+"/lock",null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



}
