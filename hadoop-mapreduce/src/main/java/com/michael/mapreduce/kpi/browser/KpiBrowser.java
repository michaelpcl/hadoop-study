package com.michael.mapreduce.kpi.browser;

import com.michael.mapreduce.firstMapreduce.WordCountMapReduce;
import com.michael.mapreduce.splitMapreduce.WordCountRun;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


/**
 * browser：用户使用的浏览器统计
 */
public class KpiBrowser {

    /**
     * Driver
     * @param args
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        // 1.Get Configuration
        Configuration conf = new Configuration();

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

        //注意这里的导包问题：用新版本 ****.mapreduce
        FileInputFormat.addInputPath(job, inPath);

        // 3.2 map class
        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 3.3 reduce class
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 3.4 output
        Path outPath = new Path(args[1]);
        FileSystem fs22 = outPath.getFileSystem(conf);
        //FileSystem fs22 = FileSystem.get(URI.create(args[1]),conf,"manager") ;
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



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\bigdata-related\\hadoop_home_bin_conf\\hadoop-common-2.6.0-bin-master");
        System.setProperty("hadoop.user.name", "manager");

        //args = new String[] { "hdfs://ns1/user/dp326007/input.txt", "hdfs://ns1/user/dp326007/output22" };

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("Usage: <input1>  <output>");
            System.exit(1);
        }
        //System.exit(ToolRunner.run(conf, new WordCountRun(), otherArgs));
        int status = 0;
        try {
            status = new KpiBrowser().run(otherArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(status ==0){
            System.out.println("正常退出");
        }
        else{
            System.out.println("异常退出");
        }
    }
}
