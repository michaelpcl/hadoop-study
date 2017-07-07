package com.michael.hdfs.hdfs1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class WriteFile {

	public static void main(String[] args) throws IOException {
		// 设置hadoop安装路径
		System.setProperty("hadoop.home.dir", "D:\\bigdata-related\\hadoop_home_bin_conf\\hadoop-common-2.2.0-bin-master");

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/home/dp326007/output/test.txt");
		FSDataOutputStream out = fs.create(path);

		out.write("michael".getBytes());
		out.write("hello!".getBytes());

		fs.close();

		System.out.println("done");
	}

}
