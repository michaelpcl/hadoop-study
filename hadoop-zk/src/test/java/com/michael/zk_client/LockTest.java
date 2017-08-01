package com.michael.zk_client;

import com.michael.zookeeper.zk_client.ZkClient;
import com.michael.zookeeper.zk_client.lock.Lock;

public class LockTest {
    public static void main(String[] args) throws InterruptedException {
        final ZkClient zk = new ZkClient("192.168.1.104:2181");
        final Lock lock = zk.getLock("/lock1");
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("1---------start");
                lock.lock();
                System.out.println("1---------getLock");
                try {
                    Thread.sleep(2000);
                    lock.lock();
                    System.out.println("1---------re getLock");
                    Thread.sleep(3000);
                    lock.unlock();
                    System.out.println("1-2--------unlock");
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("1-1--------unlock");
                lock.unlock();
            }
        }).start();
        Thread.sleep(1000);
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("2----------start");
                boolean s = lock.lock();
                System.out.println("2----------getLock---"+s);
                try {
                    Thread.sleep(3000);
                    System.out.println("2---------unlock");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                lock.unlock();
            }
        }).start();
        Thread.sleep(100000);
    }
}
