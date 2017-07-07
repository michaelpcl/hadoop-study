package com.michael.hdfs.hdfs1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class CopyFromLocalFile {
	
	public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "D:\\bigdata-related\\hadoop_home_bin_conf\\hadoop-common-2.2.0-bin-master");
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(conf);  
        Path src = new Path("d:\\326007\\Desktop\\data\\test.segmented");
        Path dst = new Path("/tmp/326007/test.segmented");
        fs.copyFromLocalFile(src, dst);  
        fs.close();  
        
        System.out.println("done");
    }
}
