package com.michael.mapreduce.kpi;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Project name hadoop-study
 * <p>
 * Package name com.michael.mapreduce.kpi
 * <p>
 * Description:
 * hadoop mapReduce模型，需要进行远程传输，为了保证传输质量，hadoop提出了消息序列化格式
 * Hadoop在运行过程中需要不停的对各个节点进行通信，而不同的数据类型对于通信影响很大， 因此，Hadoop定义了一个新的类ObjectWritable
 * java基本类型     	Hadoop封装类
 *   boolean 	BooleanWritable
 *   byte 	    ByteWritable
 *   int 	    IntWritable
 *   float 	    FloatWritable
 *   long 	    LongWritable
 *   double 	DoubleWritable
 *   等，其他数据类型见hadoop API
 * hadoop为了高效的序列化，支持自定义数据类型
 *
 * 排序任务mr中需要使用自定义的key类型，将所有结果按照kpi的数值从大到小排序
 * 此类必须实现WritableComparable泛型接口
 *
 * 重写了comparaTo,write,readFields,hashCode,toString和equals方法
 * <p>
 * Created by 326007
 * <p>
 * Created date 2017/7/10
 */
public class SortKey implements WritableComparable<SortKey> {
    //kpi指标名称
    private String kpiName;
    //指标数值
    private Integer value;

    //在反序列化时，反射机制需要调用空参构造函数
    public SortKey() {

    }
    public SortKey(String kpiName, Integer value) {
        this.kpiName = kpiName;
        this.value = value;
    }



    /**
     * 该方法在map过程之后的排序阶段会被调用,通过该方法来比较两个元素之间的顺序
     * @param o 和当前元素比较的相邻元素
     * @return 返回的结果为整型,负值表示当前元素比较大,要排在前面
     * */
    public int compareTo(SortKey o) {
        return o.value - this.value;
    }

    /**
     * 序列化对象  将java对象转换为字节，并将字节写入二进制流中
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.kpiName);
        dataOutput.writeInt(this.value);
    }

    /**
     * 反序列化，从数据流中读出对象，必须跟序列化时的顺序保持一致
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.kpiName = dataInput.readUTF();
        this.value = dataInput.readInt();
    }




    @Override
    public String toString() {
        return this.kpiName + "—" + this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SortKey sortKey = (SortKey) o;

        if (!kpiName.equals(sortKey.kpiName)) return false;
        return value.equals(sortKey.value);

    }

    @Override
    public int hashCode() {
        int result = kpiName.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }


    public String getKpiName() {
        return kpiName;
    }

    public void setKpiName(String kpiName) {
        this.kpiName = kpiName;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }
}
