package com.michael.mapreduce.kpi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 *
 * @package com.michael.mapreduce.kpi
 * @class
 * @method
 * @Description:
 * 将kpi的统计结果进行排序的mapper
 * key-in:一行的起始偏移量
 * in-value：一行的值
 * key-out：使用自定义的类型(SortKey)作为输出key
 * out-value:NullWritable
 *
 * @author 326007
 * @date 2017/7/10
 * @param
 * @return
 *
 */
public class SortMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, SortKey, NullWritable> {

    //使用自定义的类型作为key
    SortKey sortKey = new SortKey();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArr = value.toString().split("\t");
        sortKey.setKpiName(strArr[0]);
        sortKey.setValue(Integer.parseInt(strArr[1]));
        context.write(sortKey, NullWritable.get());
    }
}
