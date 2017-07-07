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
public class DemoNameNodeProImpl implements DemoNameNodePro {

    @Override
    public String getMetaData(String filePath){

        return filePath + "的block信息如下：[{blk_01,blk_02},{blk_03,blk_04}]";
    }
}
