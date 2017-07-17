package com.michael.mapreduce.partition_mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Project name hadoop-study
 * Package name com.michael.mapreduce.partition_mapreduce
 * Description:
 * 用于提交本job的一个客户端类
 *
 * Created by 326007
 * Created date 2017/7/12
 */

public class ProvinceFlowCountJobSubmitter {
    public static void main(String[] args) throws Exception {
        if(args.length<2){
            System.err.println("参数不正确：输入数据路径  输出数据路径");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ProvinceFlowCountJobSubmitter.class);

        job.setMapperClass(ProviceFlowCountMapper.class);
        job.setReducerClass(ProvinceFlowCountReducer.class);

        //map输出的kv类型与reduce输出的kv类型一致时，这两行可以省略
//      job.setMapOutputKeyClass(Text.class);
//      job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        /**
         * mapreduce分区的关键——实现自定义的partitioner
         * 通过显示指定partitioner类来让我们自定义的partitoner起作用，替换掉系统默认的hashpartitioner
         *
         */
        job.setPartitionerClass(ProvincePartitioner.class);

        /*
         * 设置本次job运行时的reduce task进程数，数量应该跟partitioner的分区数匹配
         * 默认情况下，reduce task的数量为1
         * 如果不匹配：
         *      当reduce task进程数大于partitioner的分区数，结果个数为reduce task进程数，但多余的为空。
         *      当reduce task进程数小于partitioner的分区数
         *      如果reduce task进程数为1，则所有结果在一个文件内，相当于未进行分区操作；
         *      否则，报错。
         */
        job.setNumReduceTasks(5);

        /*
         * 处理的数据文件地址
         * 数据文件处理后结果存放地址
         * 从终端获得参数
         */
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success?0:1);
    }
}
