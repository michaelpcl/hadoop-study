package com.michael.mapreduce.splitMapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
