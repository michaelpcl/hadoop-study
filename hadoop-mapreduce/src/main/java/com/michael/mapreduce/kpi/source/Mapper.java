package com.michael.mapreduce.kpi.source;

import com.michael.mapreduce.kpi.Kpi;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
    Text source = new Text();
    IntWritable one = new IntWritable(1);
    Kpi kpi = new Kpi();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        kpi = Kpi.parse(value.toString());
        System.out.println("分割后的对象：");
        System.out.println(kpi.toString());
        if (kpi.getIs_validate()) {
            source.set(kpi.getHttp_referrer());
            context.write(source, one);
        }
    }
}
