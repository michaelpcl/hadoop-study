package com.michael.zookeeper.zk_client.listener;

/**
 * Author: xiajun
 * Date: 14/5/20
 */
public class Node {
    private String path;
    private byte[] data;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
