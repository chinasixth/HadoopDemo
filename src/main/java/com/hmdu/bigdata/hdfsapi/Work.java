package com.hmdu.bigdata.hdfsapi;

import com.hmdu.bigdata.FSUtil.FSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ Author     ：liuhao
 * @ Date       ：Created in 14:18 2018/6/24
 * @ Modified By：
 * @Version: $
 */
public class Work {
    public static Logger logger = Logger.getLogger(Work.class);
    public static FileSystem fs = null;

    @Before
    public void init(){
        Configuration conf = new Configuration();

        try {
            fs = FileSystem.get(new URI("hdfs://hadoop05:9000"), conf, "root");
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } catch (URISyntaxException e) {
            logger.error(e.getMessage());
        }
    }


    // 1.列举出hdfs上某个目录下包含的文件以及目录
    @Test
    public void listAll(){
        try {
            // 当recursive=true时，迭代获取某个目录下的所有文件
            RemoteIterator<LocatedFileStatus> fileAndDir = fs.listFiles(new Path("/"), false);
            // 获取某个目录下的所有文件和子目录，不迭代
            RemoteIterator<FileStatus> dirAndFile = fs.listStatusIterator(new Path("/"));
            while (fileAndDir.hasNext()) {
                LocatedFileStatus next = fileAndDir.next();
                System.out.println(next.getPath().getName());
            }
            System.out.println("============================");
            while (dirAndFile.hasNext()) {
                FileStatus next = dirAndFile.next();
                System.out.println(next.getPath().getName());
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            FSUtils.close(fs);
        }
    }

    // 2.列举出本地文件系统的某个目录下包含的文件以及目录
    @Test
    public void getLocal(){
        Configuration conf = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(new URI("file://localhost"), conf);
            // 这里的"/"根目录，是程序所在的目录
            RemoteIterator<LocatedFileStatus> local = fileSystem.listLocatedStatus(new Path("/"));
            while (local.hasNext()) {
                LocatedFileStatus next = local.next();
                System.out.println(next.getPath().getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    // 3.列举出hdfs的所有datanode的信息
    @Test
    public void listDatanode(){
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        try {
            DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
            for (DatanodeInfo dataNodeStat : dataNodeStats) {
                System.out.println(dataNodeStat.getAdminState());
                System.out.println(dataNodeStat.getName());
                System.out.println("==========================");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 4.合并上传文件
    @Test
    public void combineFile(){
        FSDataOutputStream out = null;
        try {
            out = fs.create(new Path("/1combine2.dat"), false);
            String[] srcs = { "E:\\hadoopdata/test01.txt", "E:\\hadoopdata/test02.txt" };
            for (String src : srcs) {
                FileInputStream fin = new FileInputStream(new File(src));
                IOUtils.copyBytes(fin, out, 4096);
                fin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void overrideFile(){
        FSDataOutputStream fsout = null;
        try {
            // true可以对已经存在的文件进行重写，false会发生文件存在异常
            fsout = fs.create(new Path("/1combine2.dat"), true);
            fsout.write("liuhao\n".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fsout.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            FSUtils.close(fs);
        }
    }

}
