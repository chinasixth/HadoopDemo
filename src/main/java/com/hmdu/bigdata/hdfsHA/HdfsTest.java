package com.hmdu.bigdata.hdfsHA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:02 2018/6/27
 */
public class HdfsTest {
    public static Logger logger = Logger.getLogger(com.hmdu.bigdata.hdfsapi.HdfsTest.class);
    public static FileSystem fs = null;

    /**
     * Before修饰的代码，就是执行测试代码之前一定会执行的代码
     */
    @Before
    public void init(){
        // 获取配置对象
        Configuration conf = new Configuration();

        try {
            // hdfs的HA集群的连接方法：
            // 1.程序中配置参数，利用配置对象conf
            // 2.不做程序的配置，直接将集群的配置文件hdfs-site.xml、core-site.xml放到resources目录
            //   当new Configuration时，按照顺序加载配置文件(之前讲过)
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    @Test
    public void listAllFile() {
        try {
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
            while (iterator.hasNext()) {
                LocatedFileStatus fileStatus = iterator.next();
                System.out.println(fileStatus.getPath().getName());
                System.out.println(fileStatus.getLen());
                System.out.println(fileStatus.getReplication());
                System.out.println(fileStatus.getPermission());

                BlockLocation[] blockLocations = fileStatus.getBlockLocations();
                for (BlockLocation blockLocation : blockLocations) {
                    System.out.println(blockLocation.getOffset());

                    String[] hosts = blockLocation.getHosts();
                    for (String hostname : hosts) {
                        System.out.println(hostname);
                    }
                }
                System.out.println("=======================================");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
