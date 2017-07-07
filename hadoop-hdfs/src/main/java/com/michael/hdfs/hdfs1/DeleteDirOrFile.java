package com.michael.hdfs.hdfs1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class DeleteDirOrFile {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(conf);  
          
        Path path = new Path("/user/local/");  
        fs.delete(path);  
        fs.close();
        
        System.out.println("done");
	}

}
