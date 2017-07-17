package com.michael.mapreduce.partition_mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import java.io.IOException;

/**
 * Project name hadoop-study
 * <p>
 * Package name com.michael.mapreduce.partition_mapreduce
 * <p>
 * Description:
 * <p>
 * Created by 326007
 * <p>
 * Created date 2017/7/12
 */
public class ProviceFlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    /*
     * 定义成成员变量，这样可以提高效率，减少垃圾回收。
     */
    private Text k=new Text();
    private FlowBean bean=new FlowBean();

    @Override
    protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = StringUtils.split(line, '\t');
        String phone=fields[1];
        long upFlow=Long.parseLong(fields[fields.length-3]);
        long downFlow=Long.parseLong(fields[fields.length-2]);

        k.set(phone);
        //set方法中直接将总流量计算出来了
        bean.set(upFlow, downFlow);
        context.write(k, bean);
    }
}