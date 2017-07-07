package com.michael.hdfs.hdfs1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class MakeDir {
	public static void main(String[] args) throws IOException {
		// 设置hadoop安装路径
		System.setProperty("hadoop.home.dir", "D:\\bigdata-related\\hadoop_home_bin_conf\\hadoop-common-2.2.0-bin-master");
		String des = "/user/dp326007/output";
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(des),conf,"manager");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Path path = new Path(des);
		fs.create(path);
		fs.close();

		System.out.println("done");
	}
}
