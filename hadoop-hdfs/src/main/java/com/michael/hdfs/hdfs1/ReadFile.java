package com.michael.hdfs.hdfs1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class ReadFile {
	public static void main(String[] args) throws IOException {
		System.setProperty("hadoop.home.dir", "D:\\bigdata-related\\hadoop_home_bin_conf\\hadoop-common-2.6.0-bin-master");
		System.setProperty("hadoop.user.name", "manager");

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		String childPath = "";
		Path foldPath = new Path("/user/dp326007/output22");
		List<String> listPath = GetAllChildFile.getFile(foldPath,fs);

		FileSystem fs22 = FileSystem.get(conf);
		for(String strPath : listPath){
			Path path = new Path(strPath);
			if (fs.exists(path)) {
				FSDataInputStream in = fs.open(path);
				FileStatus status = fs.getFileStatus(path);
				byte[] buffer = new byte[Integer.parseInt(String.valueOf(status.getLen()))];
				in.readFully(0, buffer);
				in.close();
				fs.close();
				System.out.println(buffer.toString());
				String s = new String(buffer);
				System.out.println(s.toString());
			}
		}
		System.out.println("done");
	}
}
