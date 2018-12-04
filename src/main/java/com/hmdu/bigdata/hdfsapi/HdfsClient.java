package com.hmdu.bigdata.hdfsapi;

import com.hmdu.bigdata.FSUtil.FSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    public static Logger logger = Logger.getLogger(HdfsClient.class);
    public static FileSystem fs = null;

    /*
     * 静态代码块封装公共代码部分
     * */
    static {

        // 获取配置对象
        Configuration conf = new Configuration();

        try {
            fs = FileSystem.get(new URI("hdfs://192.168.216.115:9000"), conf, "root");
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } catch (URISyntaxException e) {
            logger.error(e.getMessage());
        }

    }

    /*
     * 批量上传文件
     * */
    public static void main(String[] args) {
        Path[] srcs = {
                new Path("E:\\hadoopdata\\test01.txt"),
                new Path("E:\\hadoopdata\\test02.txt"),
                new Path("E:\\hadoopdata\\shell多线程.txt")
        };

        try {
            fs.copyFromLocalFile(false, true, srcs, new Path("/"));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        FSUtils.close(fs);
    }

}
