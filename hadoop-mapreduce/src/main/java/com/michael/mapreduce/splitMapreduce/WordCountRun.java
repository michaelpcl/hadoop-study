package com.michael.mapreduce.splitMapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * Project name hdfs-to-activemq
 * <p>
 * Package name com.deppon.study_self.wordcount
 * <p>
 * Description:
 * <p>
 * Created by 326007
 * <p>
 * Created date 2017/6/30
 */
public class WordCountRun extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(this.getClass());
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        //输出地址
        Path outPath = new Path(strings[1]);
        FileSystem fs = outPath.getFileSystem(conf);
//        FileSystem fs22 = FileSystem.get(URI.create(args[1]),conf,"manager") ;
        //判断hdfs目录下是否已经存在output文件夹，如果存在则删除（因为output要求是不存在的）
        if(fs.exists(outPath)){
            fs.delete(outPath, true) ;
        }
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        //提交作业
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)throws Exception{
        System.setProperty("hadoop.home.dir", "D:\\bigdata-related\\hadoop_home_bin_conf\\hadoop-common-2.6.0-bin-master");
        System.setProperty("hadoop.user.name", "manager");

        args = new String[] { "hdfs://ns1/user/dp326007/input.txt", "hdfs://ns1/user/dp326007/output22" };

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("Usage: <input1>  <output>");
            System.exit(1);
        }
        //System.exit(ToolRunner.run(conf, new WordCountRun(), otherArgs));
        int status = new WordCountRun().run(otherArgs);
        if(status ==0){
            System.out.println("正常退出");
        }
        else{
            System.out.println("异常退出");
        }
    }
}
