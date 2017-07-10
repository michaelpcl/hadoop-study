package com.michael.mapreduce.kpi.browser;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 * 对客户端各浏览器使用量进行统计
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
    //定义reduce的输出value
    IntWritable resCount = new IntWritable();

    /**
     *
     * @param key
     * key(客户端浏览器的名称)
     * @param values
     * shuffle后按照key(客户端浏览器的名称)进行分组，相同浏览器名称对应的初始值
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        resCount.set(sum);
        //定义reduce的输出
        context.write(key, resCount);
    }
}
