package com.michael.mapreduce.kpi.browser;

import com.michael.mapreduce.kpi.Kpi;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    //用户使用的浏览器
    Text browser = new Text();
    //定义格式化后的对象
    Kpi kpi = new Kpi();

    /**
     *
     * @param key
     * key in 一行起始偏移量
     * Text value 一行文本内容
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //对输入的一行文本按照自定义格式化方式格式化
        kpi = Kpi.parse(value.toString());
        System.out.println("分割后的对象：");
        System.out.println(kpi.toString());
        if (kpi.getIs_validate()) {
            //指定mapper的输出的key,类型为hadoop数据类型之一：Text
            browser.set(kpi.getUser_agent());
            context.write(browser, new IntWritable(1));
        }
    }
}
