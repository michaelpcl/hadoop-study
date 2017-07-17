package com.michael.mapreduce.partition_mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Project name hadoop-study
 * Package name com.michael.mapreduce.partition_mapreduce
 * Description:
 * 按省份分区
 * 将手机号码归属到对应的省份
 * KEY为Mapper输出的key
 * VALUE为Mapper输出的value
 *
 * Created by 326007
 * Created date 2017/7/12
 */

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    private static HashMap<String, Integer> provinceMap = new HashMap<String, Integer>();

    //在partitioner初始化的时候就将外部字典数据一次性加载到本地内存中
    static{
        //加载外部的字典数据到本地内存
        provinceMap.put("136", 0);
        provinceMap.put("137", 1);
        provinceMap.put("138", 2);
        provinceMap.put("139", 3);
    }

    //numReduceTasks为reduce task进程的数量
    @Override
    public int getPartition(Text key, FlowBean value, int numReduceTasks) {
        //取手机号的前缀
        String prefix =key.toString().substring(0, 3);
        //从字典数据中查询归属地的分区号
        Integer provinceNum = provinceMap.get(prefix);
        if(provinceNum==null) provinceNum=4;
        return provinceNum;
    }

}