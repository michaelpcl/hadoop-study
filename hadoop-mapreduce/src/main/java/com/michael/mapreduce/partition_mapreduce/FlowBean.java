package com.michael.mapreduce.partition_mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Project name hadoop-study
 * Package name com.michael.mapreduce.partition_mapreduce
 * Description:
 * 手机流量统计
 *
 * 自定义的类型需要实现Writable
 * 如果需要排序需要实现WritableComparable
 *
 * Created by 326007
 * Created date 2017/7/12
 */
//public class FlowBean implements WritableComparable<FlowBean>{

public class FlowBean implements Writable{
    //上行流量
    private long upFlow;
    //下行流量
    private long downFlow;
    //总流量
    private long sumFlow;

    //因为反射机制的需要，必须定义一个无参构造函数
    public FlowBean(){};

    public FlowBean(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow+downFlow;
    }

    public void set(long upFlow, long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow+downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }
    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }
    public long getDownFlow() {
        return downFlow;
    }
    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }
    public long getSumFlow() {
        return sumFlow;
    }
    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /*
     * 反序列化方法：从数据字节流中逐个恢复出各个字段
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow=in.readLong();
        downFlow=in.readLong();
        sumFlow=in.readLong();
    }

    /*
     * 序列化方法：将我们要传输的数据序列成字节流
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+sumFlow;
    }

    /**
     * 排序需要实现
     * @param o
     * @return
     */
    /*@Override
    public int compareTo(FlowBean o) {
        //倒序排序
        return this.sumFlow>o.getSumFlow()?-1:1;
    }*/

}
