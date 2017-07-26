package com.michael;

import com.michael.zookeeper.simple_client.ZookeeperInstance;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Project name hadoop-study
 * <p>
 * Package name com.michael
 * <p>
 * Description:
 * <p>
 * Created by 326007
 * <p>
 * Created date 2017/7/26
 */
public class ZookeeperInstanceTest {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        ZookeeperInstance zookeeperInstance = new ZookeeperInstance();

        String host = "192.168.68.163:2181,192.168.68.163:2182,192.168.68.163:2183";

        zookeeperInstance.connect(host);
        System.out.println("1、--------connect zookeeper ok-----------\n");

        boolean isExists = zookeeperInstance.exists("/test");
        if (isExists) {
            zookeeperInstance.deleteNode("/test", -1);
            System.out.println("2、--------delete znode ok-----------\n");
        }
        System.out.println("3、--------exists znode ok-----------\n");


        byte[] data = {1, 2, 3, 4, 5};
        String result = zookeeperInstance.createNode("/test", data);
        System.out.println(result);
        System.out.println("4、--------create znode ok-----------\n");


        List<String> children = zookeeperInstance.getChildren("/");
        for (String child : children) {
            System.out.println(child);
        }
        System.out.println("5、--------get children znode ok-----------\n");


        byte[] nodeData = zookeeperInstance.getData("/test");
        System.out.println(Arrays.toString(nodeData));
        System.out.println("6、--------get znode data ok-----------\n");


        data = "test data".getBytes();
        zookeeperInstance.setData("/test", data, 0);
        System.out.println("7、--------set znode data ok-----------\n");

        nodeData = zookeeperInstance.getData("/test");
        System.out.println(Arrays.toString(nodeData));
        System.out.println("8、--------get znode new data ok-----------\n");

        zookeeperInstance.closeConnect();
        System.out.println("9、--------close zookeeper ok-----------\n");
    }
}
