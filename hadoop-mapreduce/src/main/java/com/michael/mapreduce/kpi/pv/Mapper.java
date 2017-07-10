package com.michael.mapreduce.kpi.pv;

import com.michael.mapreduce.kpi.Kpi;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    Text request_page = new Text();
    Kpi kpi = new Kpi();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        kpi = Kpi.parse(value.toString());
        System.out.println("分割后的对象：");
        System.out.println(kpi.toString());
        if (kpi.getIs_validate()) {
            request_page.set(kpi.getRequest_page());
            context.write(request_page, new IntWritable(1));
        }
    }
}
