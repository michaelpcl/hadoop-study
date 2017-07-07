package com.michael.namenode;

/**
 * Project name hadoop-study
 * <p>
 * Package name com.michael.namenode
 * <p>
 * Description:
 * <p>
 * Created by 326007
 * <p>
 * Created date 2017/7/7
 */
public interface DemoNameNodePro {
    /**
     * 定义通讯双方一致的版本号
     */
     public static final long versionID = 1L;

    /**
     * 定义通讯双方可以调用的方法
     * @param filePath
     * @return
     */
    public String getMetaData(String filePath);

}
