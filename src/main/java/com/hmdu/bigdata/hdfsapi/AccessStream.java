package com.hmdu.bigdata.hdfsapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * open是针对hdfs的输入流，也就是从hdfs文件系统输出数据
 * create是针对hdfs的输出流，也就是向hdfs文件系统输入数据
 */

public class AccessStream {

    public static Logger logger = Logger.getLogger(HdfsTest.class);
    public static FileSystem fs = null;

    @Before
    public void init(){
        // 获取配置对象
        Configuration conf = new Configuration();

        try {
            fs = FileSystem.get(new URI("hdfs://192.168.216.115:9000"), conf, "root");
        } catch (IOException | URISyntaxException e) {
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * 流下载文件
     */
    @Test
    public void downloadFile(){
        FSDataInputStream open = null;
        FileOutputStream fileOutputStream = null;

        try {
            // 针对hdfs上的文件，创建一个文件的输入流
            open = fs.open(new Path("/tmp/123.dat"));

            // 针对本地文件创建一个文件输出流
            fileOutputStream = new FileOutputStream("e:/hadoopdata/123.txt");

            // 将输入流中的数据传输的输出流
            // hadoop提供了IOUtils，用来转换流，第三个参数是一次处理多少数据
            IOUtils.copyBytes(open, fileOutputStream, 4096);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (open != null) {
                try {
                    open.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (fileOutputStream != null) {
                try {
                    fileOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 流上传文件
     */
    @Test
    public void uploadFile(){
        FSDataOutputStream fsDataOutputStream = null;
        FileInputStream fileInputStream = null;
        try {
            // 创建一个针对hdfs文件的输出流
            fsDataOutputStream = fs.create(new Path("/tmp/456.dat"));

            // 创建一个针对本地文件的输入流
            fileInputStream = new FileInputStream(new File("e:/hadoopdata/123.txt"));
            IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 4096);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fsDataOutputStream != null) {
                try {
                    fsDataOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * seek定位读取文件内容
     */
    @Test
    public void testCat(){
        FSDataInputStream fsin = null;

        try {
            // 获取一个针对hdfs的输入流，也就是从hdfs中输入到fs
            fsin = fs.open(new Path("/tmp/456.dat"));

            // 定位,从第几个字符开始读，字符下标从0开始
            fsin.seek(6L);

            // 第四个参数表示是否要关闭流，第三个参数表示读几个
            IOUtils.copyBytes(fsin, System.out, 2L, true);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fsin != null) {
                try {
                    fsin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 随机读取文件内容
     * hdfs允许随机的文件读取，可以很方便的读取指定长度
     * 这种场景主要用于上层运算框架的高并发处理数据
     */
    @Test
    public void randomAccess() {
        FSDataInputStream fsin = null;
        FileOutputStream fileOutputStream = null;

        try {
            // 先获取一个针对hdfs的输入流
            fsin = fs.open(new Path("/shell多线程.txt"));

            // 指定偏移量offset
            fsin.seek(3L);

            // 构造一个针对本地文件的输出流
            fileOutputStream = new FileOutputStream("e:/hadoopdata/shell.txt");

            // 使用工具类对流的内容进行转换
            IOUtils.copyBytes(fsin, fileOutputStream, 6L, true);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fsin != null) {
                try {
                    fsin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (fileOutputStream != null) {
                try {
                    fileOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}


