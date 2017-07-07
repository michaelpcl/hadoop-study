package com.michael.hdfs.hdfs1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetAllChildFile {
	static Configuration conf = new Configuration();

    public static void main(String[] args)throws IOException {
        System.setProperty("hadoop.home.dir", "D:\\bigdata-related\\hadoop_home_bin_conf\\hadoop-common-2.6.0-bin-master");
        System.setProperty("hadoop.user.name", "manager");

        FileSystem fs = FileSystem.get(conf);  
        Path path = new Path("/user/dp326007/output22");
        getFile(path,fs);  
        //fs.close();
        System.out.println("done");
    }  
      
    public static List<String> getFile(Path path, FileSystem fs) throws IOException {
        List<String> listPath = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(path);
        for(int i=0;i<fileStatus.length;i++){  
            if(fileStatus[i].isDir()){  
                Path p = new Path(fileStatus[i].getPath().toString());  
                getFile(p,fs);  
            }else{
                if(fileStatus[i].getPath() != null){
                    System.out.println(fileStatus[i].getPath().toString());
                    listPath.add(fileStatus[i].getPath().toString());
                }
            }  
        }
        return listPath;
    }  
}
