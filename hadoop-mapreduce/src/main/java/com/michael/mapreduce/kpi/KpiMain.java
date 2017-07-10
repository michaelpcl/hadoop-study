package com.michael.mapreduce.kpi;

import com.michael.mapreduce.kpi.browser.KpiBrowser;
import com.michael.mapreduce.kpi.ips.IpsCount;
import com.michael.mapreduce.kpi.pv.PvDriver;
import com.michael.mapreduce.kpi.source.SourcePv;
import com.michael.mapreduce.kpi.time.TimePv;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Project name hadoop-study
 * <p>
 * Package name com.michael.mapreduce.kpi
 * <p>
 * Description:
 * <p>
 * Created by 326007
 * <p>
 * Created date 2017/7/10
 */
public class KpiMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\bigdata-related\\hadoop_home_bin_conf\\hadoop-common-2.6.0-bin-master");
        System.setProperty("hadoop.user.name", "manager");

        args = new String[] { "hdfs://ns1/user/dp326007/kpi_in/*", "hdfs://ns1/user/dp326007/kpi_out" };

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("Usage: <input1>  <output>");
            System.exit(1);
        }
        //System.exit(ToolRunner.run(conf, new WordCountRun(), otherArgs));
        /**
         * 用户使用的浏览器统计
         */
        int browserStatus = 0;
        try {
            browserStatus = new KpiBrowser().run(otherArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(browserStatus ==0){
            System.out.println("browser正常退出");
        }
        else{
            System.out.println("browser异常退出");
        }

        /**
         * 每个页面独立的ip访问数
         */
        int ipCountStatus = 0;
        try {
            ipCountStatus = new IpsCount().run(otherArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(ipCountStatus ==0){
            System.out.println("ipCount正常退出");
        }
        else{
            System.out.println("ipCount异常退出");
        }

        /**
         * 单页面浏览量统计
         */
        int pvStatus = 0;
        try {
            pvStatus = new PvDriver().run(otherArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(pvStatus ==0){
            System.out.println("pv正常退出");
        }
        else{
            System.out.println("pv异常退出");
        }

        /**
         * 访问来源统计
         * SourcePv
         */
        int SourcePvStatus = 0;
        try {
            SourcePvStatus = new SourcePv().run(otherArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(SourcePvStatus ==0){
            System.out.println("SourcePv正常退出");
        }
        else{
            System.out.println("SourcePv异常退出");
        }

        /**
         * 相同时间段内，用户访问统计
         */
        int timePvStatus = 0;
        try {
            timePvStatus = new TimePv().run(otherArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(timePvStatus ==0){
            System.out.println("TimePv正常退出");
        }
        else{
            System.out.println("TimePv异常退出");
        }
    }
}
