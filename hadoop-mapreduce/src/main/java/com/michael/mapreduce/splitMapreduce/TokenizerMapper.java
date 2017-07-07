package com.michael.mapreduce.splitMapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

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
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
