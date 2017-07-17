package com.michael.mapreduce.partition_mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Project name hadoop-study
 * <p>
 * Package name com.michael.mapreduce.partition_mapreduce
 * <p>
 * Description:
 * <p>
 * Created by 326007
 * <p>
 * Created date 2017/7/12
 */
public class ProvinceFlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean bean=new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values,Context context)throws IOException, InterruptedException {
        long upFlowSum=0;
        long downFlowSum=0;
        for(FlowBean value : values){
            upFlowSum+=value.getUpFlow();
            downFlowSum+=value.getDownFlow();
        }
        bean.set(upFlowSum, downFlowSum);
        context.write(key, bean);
    }
}
