package com.michael.mapreduce.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

/**
 * Project name hadoop-study
 * Package name com.michael.mapreduce.friends
 * Description:
  stepOne的执行结果作为第二步骤的输入

 A	I,K,C,B,G,F,H,O,D
 B	A,F,J,E
 C	A,E,B,H,F,G,K
 D	G,C,K,A,L,F,E,H
 E	G,M,L,H,A,F,B,D
 F	L,M,D,C,G,A
 G	M
 H	O
 I	O,C
 J	O
 K	B
 L	D,E
 M	E,F
 O	A,H,I,J,F

 * Created by 326007
 * Created date 2017/7/18
 */
public class FriendsStepTwo {

    public static class FriendsStepTwoMapper extends Mapper<LongWritable,Text,Text,Text> {
        Text k = new Text();
        Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            /**
             * tab按键的转义符号
             * 上一个步骤的reduce的输出就是key + tab + values
             */
            String[] friend_persons = line.split("\t");
            String friend = friend_persons[0];
            String[] persons = friend_persons[1].split(",");
            //排序
            Arrays.sort(persons);
            for(int i = 0 ; i<persons.length-2;i++){
                for(int j=i+1;j<persons.length-1;j++){
                    context.write(new Text(persons[i]+"-"+persons[j]),new Text(friend));
                }
            }
        }
    }


    public static class FriendsStepTwoReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text person_person, Iterable<Text> friends, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for(Text friend : friends){
                sb.append(friend).append(" ");
            }
            context.write(person_person,new Text(sb.toString()));
        }
    }



    /**
     * 执行提交的任务
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //conf.set("mapreduce.framework.name","yarn");


        //默认就是local模式
        conf.set("mapreduce.framework.name","local");
        conf.set("mapreduce.jobtracker.address","local");
        conf.set("fs.defaultFS","file:///");


        Job wcjob = Job.getInstance(conf);

        wcjob.setJarByClass(this.getClass());

        //如果从本地拷贝，是不行的，这时需要使用setJar
//        wcjob.setJarByClass(Rjoin.class);

        wcjob.setMapperClass(FriendsStepTwoMapper.class);
        wcjob.setReducerClass(FriendsStepTwoReducer.class);

        //设置我们的业务逻辑Mapper类的输出key和value的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(Text.class);


        //设置我们的业务逻辑Reducer类的输出key和value的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(Text.class);


        //如果不设置InputFormat，默认就是使用TextInputFormat.class
//        wcjob.setInputFormatClass(CombineFileInputFormat.class);
//        CombineFileInputFormat.setMaxInputSplitSize(wcjob,4194304);
//        CombineFileInputFormat.setMinInputSplitSize(wcjob,2097152);


        // 3.1 Input
        Path inPath = new Path(args[0]);
        FileSystem fs = inPath.getFileSystem(conf);

        // 3.4 output
        Path outPath = new Path(args[1]);
        FileSystem fs22 = outPath.getFileSystem(conf);
//        FileSystem fs22 = FileSystem.get(URI.create(args[1]),conf,"manager") ;
        //判断hdfs目录下是否已经存在output文件夹，如果存在则删除（因为output要求是不存在的）
        if(fs22.exists(outPath)){
            fs22.delete(outPath, true) ;
        }

        //指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob, inPath);
        //指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(wcjob, outPath);


        // 4.submit job
        boolean isSuccess = wcjob.waitForCompletion(true);
        //工作提交成功返回true --> 0
        return isSuccess ? 0 : 1;
    }



    public static void main(String[] args) {
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
        args = new String[] { "file:///d:/326007/Desktop/data/friends/stepOne/output/", "file:///d:/326007/Desktop/data/friends/stepTwo/output" };


        System.setProperty("hadoop.home.dir", "D:\\bigdata-related\\hadoop_home_bin_conf\\hadoop-common-2.6.0-bin-master");
        System.setProperty("HADOOP_USER_NAME", "manager");

        // run job
        int status = 0;
        try {
            status = new FriendsStepTwo().run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // exit program
        System.exit(status);
    }




}
