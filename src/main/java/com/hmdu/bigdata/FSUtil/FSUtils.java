package com.hmdu.bigdata.FSUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class FSUtils {

    // 定义一个日志类,将抛出的异常打印到日志中
    public static Logger logger = Logger.getLogger(FSUtils.class);


    /*
     * 获取文件系统对象
     * */
    public static FileSystem getFileSystem() {
        // 获取配置对象
        Configuration conf = new Configuration();
        FileSystem fs = null;

        try {
            // 第三个参数指定用户，有可能会没有访问权限
            fs = FileSystem.get(new URI("hdfs://192.168.216.115:9000"), conf, "root");
        } catch (IOException | URISyntaxException | InterruptedException e) {
            logger.error(e.getMessage());
        }

        return fs;
    }

    /*
     * 关闭资源
     * */
    public static void close(FileSystem fs) {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
