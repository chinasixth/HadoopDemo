package com.hmdu.bigdata.hdfsapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsDemo {

    // psvm快速生成main方法
    public static void main(String[] args) throws IOException, URISyntaxException {

        /*
         * 如果hdfs开启了用户权限，访问hdfs需要指定用户(客户端操作shell命令)
         * 用户权限问题
         * 1. 设置-DHADOOP_USER_NAME=root  -D表示设置参数
         * 2. 设置System系统环境变量
         *    System.setProperty("HADOOP_USER_NAME", "root");
         *    注意：在获取文件系统对象之前设置
         * 3. 在获取文件系统对象的时候直接指定用户
         *    FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.216.115:9000"), conf , "root");
         * */

        /*
         * 构造配置参数对象，设置了一个参数，这个参数是要访问的hdfs的uri，也就是要访问的文件系统
         * 从FileSystem.get方法中，根据配置对象能够分辨出是要哪种类型的系统的哪种文件系统
         * 从而生成一个能够操作分布式文件系统的客户端
         *
         * new Configuration的时候，先加载jar包中的hdfs-default.xml,再加载classpath中的hdfs-site.xml
         * 配置参数的优先级：
         * 1. 用户在程序中设置的参数级别最高
         * 2. 用户自定义(classpath)的配置文件里面参数级别次之
         * 3. 服务器默认的配置文件中的参数级别最低
         * */

        /*
         * 文件系统的获取方式：
         * 1. FileSystem fs = FileSystem.get(conf);
         * 2. FileSystem fs = FileSystem.get(new URI("hdfs://192.168.216.115:9000"), conf , "root");
         * 3. FileSystem fs = FileSystem.newInstance("conf");
         * */

        // 从本地上传文件到hdfs文件系统
        // copyFromLocalFile();

        // 从hdfs文件系统下载文件到本地
        copyToLocalFile();
    }

    /*
     * 从本地上传文件到hdfs文件系统
     * */
    public static void copyFromLocalFile() throws IOException {
        // 1.创建配置对象
        Configuration conf = new Configuration();

        //2.设置连接hdfs的参数
        conf.set("fs.defaultFS", "hdfs://192.168.216.115:9000");

        // 3.获取文件系统的操作对象（alt+enter）
        FileSystem fs = FileSystem.get(conf);

        // 4.调用方法
        fs.copyFromLocalFile(new Path("E:\\QF\\hadoop\\shell多线程.txt"), new Path("/"));

        // 5.关闭资源
        fs.close();

        System.out.print("finished...");
    }

    public static void copyToLocalFile() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.216.115:9000"), conf);

        fs.copyToLocalFile(new Path("/shell多线程.txt"), new Path("E:\\hadoopdata"));
        // 使用四个参数的方法：
        // 第四个useRawLocalFileSystem参数：是否使用原生的Java程序操作本地文件系统，
        // 如果为false，则使用winutils.exe;如果为true使用原生Java操作
        // 也就是要么安装无jar版的hadoop环境，要么指定第四个参数为true
        // fs.copyToLocalFile(false, new Path("/shell多线程.txt"), new Path("E:\\hadoopdata"), true);

        fs.close();

        System.out.print("finished...");
    }

}
