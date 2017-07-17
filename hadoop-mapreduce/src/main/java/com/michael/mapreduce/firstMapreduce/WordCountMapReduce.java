package com.michael.mapreduce.firstMapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by 389644 on 2017/6/30.
 */
public class WordCountMapReduce {
    /**
     * Mapper class : public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text mapOutputKey = new Text();
        private final IntWritable mapOutputValue = new IntWritable(1);

        /**
         * Mapper方法是提供给map-task进程调用的，map-task进程每读取一行文本就调用一次我们自定义的mapper方法
         * 传递的参数说明:
         * @param key
         * 一行的起始偏移量
         * @param value
         * 一行文本的内容
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // line value
            String lineValue = value.toString();

            // split
            String[] strs = lineValue.split("\t");

            // iterator
            for (String str : strs) {
                mapOutputKey.set(str);
                context.write(mapOutputKey, mapOutputValue);
            }
        }
    }

    /**
     * Reducer class
     *
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable outputValue = new IntWritable();

        /**
         * reduce方法是提供给reduce-task进程调用的
         * reduce-task会将shuffle阶段过来的大量的k-v数据进行聚合，聚合的机制是将key相同的k-v数据聚合为一组，然后reduce-task对每一组聚合k-v调用一次我们的reduce程序
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // temp : sum
            int sum = 0;

            // iterator
            for (IntWritable value : values) {
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }

    }

    /**
     * Driver
     *
     * @param args
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        // 1.Get Configuration
        Configuration conf = new Configuration();
        /**
         * 设置运行模式
         * 本地运行
         * 集群运行模式
         * 设置了如下的两个参数，就是设置集群运行模式，否则就是本地模式
         */
        //conf.set("mapreduce.framework.name","yarn");
        //conf.set("yarn.resourcemanager.hostname","host主机");

        // 2.create job
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        /**
         * 指定本job所在的jar包
         * 分发时会将该jar分发到集群
         */
        job.setJarByClass(this.getClass());



        // 3.set job
        // Input --> map --> reduce --> output
        // 3.1 Input
        Path inPath = new Path(args[0]);
        FileSystem fs = inPath.getFileSystem(conf);
        //判断hdfs目录下是否已经存在output文件夹，如果存在则删除（因为output要求是不存在的）
        if(fs.exists(inPath)){
            //fs.delete(inPath, true) ;
        }
        //注意这里的导包问题：用新版本 ****.mapreduce
        FileInputFormat.addInputPath(job, inPath);

        // 3.2 map class
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 3.3 reduce class
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 3.4 output
        Path outPath = new Path(args[1]);
        FileSystem fs22 = outPath.getFileSystem(conf);
//        FileSystem fs22 = FileSystem.get(URI.create(args[1]),conf,"manager") ;
        //判断hdfs目录下是否已经存在output文件夹，如果存在则删除（因为output要求是不存在的）
        if(fs22.exists(outPath)){
            fs22.delete(outPath, true) ;
        }

        FileOutputFormat.setOutputPath(job, outPath);

        // 4.submit job
        boolean isSuccess = job.waitForCompletion(true);
        //工作提交成功返回true --> 0
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        /**
         * 地址是：hdfs://主机名:8020/input
         * 这样的模式也是本地模式，只是会从hdfs上把数据弄到本地
         * 区分本地和集群运行模式，主要看配置文件
         */
        //args = new String[] { "hdfs://ns1/user/dp326007/input.txt", "hdfs://ns1/user/dp326007/output11" };

        /**
         * 本地测试，将文件放置到本地
         * 不能直接写路径
         * 需要file:/// + 路径的形式
         */
        args = new String[] { "file:///d:/326007/Desktop/data/local_model/input/input.txt", "file:///d:/326007/Desktop/data/local_model/output" };


        System.setProperty("hadoop.home.dir", "D:\\bigdata-related\\hadoop_home_bin_conf\\hadoop-common-2.6.0-bin-master");
        System.setProperty("HADOOP_USER_NAME", "manager");

        // run job
        int status = new WordCountMapReduce().run(args);

        // exit program
        System.exit(status);
    }
}
