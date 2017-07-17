package com.michael.mapreduce.partition_mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Project name hadoop-study
 * Package name com.michael.mapreduce.partition_mapreduce
 * Description:
 * 对流量分区，倒序排序
 * 这里实现在一个类中
 * Created by 326007
 * Created date 2017/7/12
 */
public class FlowCountSort {
    /*
     * 这是流量汇总排序中的第二个步骤，处理的数据是汇总步骤的输出结果
     */
    public static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
        @Override
        protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
            String line=value.toString();
            String[] fields = StringUtils.split(line, '\t');

            String phone=fields[0];
            long upFlow=Long.parseLong(fields[1]);
            long downFlow=Long.parseLong(fields[2]);

            // 把流量信息作为key，以便排序的结果符合我们的需求
            context.write(new FlowBean(upFlow,downFlow), new Text(phone));
        }
    }


    /**
     * 这个reduce,只是将汇总后的结果显示出来，对调了一下输出的内容
     * 有的可以不需要reduce程序，可以参考KPI中的指标排序（没有reduce程序）
     */
    public static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
        /*
         * 在流量汇总排序的步骤中，reducer不需要做汇总 而且reduce方法被调用时拿到的数据就是一个kv
         */
        @Override
        protected void reduce(FlowBean key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
            context.write(values.iterator().next(), key);
        }
    }


    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowCountSort.class);

        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        /*
         * 注意：这里的数据文件是上一步骤流量汇总之后的结果
         */
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}