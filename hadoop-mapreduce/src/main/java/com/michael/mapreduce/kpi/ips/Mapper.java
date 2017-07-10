package com.michael.mapreduce.kpi.ips;

import com.michael.mapreduce.kpi.Kpi;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 * 页面访问独立IP数mapper
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    Kpi kpi = new Kpi();
    //定义mapper的输出key
    Text requestPage = new Text();
    //定义mapper的输出的value
    Text remoteAddr = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //对输入的一行文本按照自定义格式化方式格式化
        kpi = Kpi.parse(value.toString());
        System.out.println("分割后的对象：");
        System.out.println(kpi.toString());
        if (kpi.getIs_validate()) {
            requestPage.set(kpi.getRequest_page());
            remoteAddr.set(kpi.getRemote_addr());
            context.write(requestPage, remoteAddr);
        }
    }
}
