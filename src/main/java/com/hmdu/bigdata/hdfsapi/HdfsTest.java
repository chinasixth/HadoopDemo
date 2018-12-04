package com.hmdu.bigdata.hdfsapi;

import com.hmdu.bigdata.FSUtil.FSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsTest {
    public static Logger logger = Logger.getLogger(HdfsTest.class);
    public static FileSystem fs = null;

    /**
     * Before修饰的代码，就是执行测试代码之前一定会执行的代码
     */
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
     * 这是一个Test方法
     * 在hdfs文件系统中创建一个目录
     */
    @Test
    public void mkdir(){
        boolean bool = false;
        try {
            bool = fs.mkdirs(new Path("/wordcount"));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        if (bool) {
            System.out.println("创建成功！");
        } else{
            System.out.println("创建失败！");
        }

        FSUtils.close(fs);
    }

    /**
     * 永久性删除文件或目录，如果是一个空目录或文件，recursive的值就会被忽略，
     * 只用当recursive=true时，一个非空目录以及内容才会被永久删除
     * recursive是递归删除的意思
     */
    @Test
    public void delete(){

        try {
//            if (fs.delete(new Path("/out"), false)){  //如果设置为false且目录不为空，会报异常
//                System.out.println("false非递归删除目录成功");
//            } else{
//                System.out.println("false非递归删除目录失败");
//            }

            if(fs.delete(new Path("/out"), true)){
                System.out.println("true递归删除目录成功");
            } else{
                System.out.println("true递归删除目录失败");
            }

            if(fs.delete(new Path("/test01.txt"), false)){
                System.out.println("false删除存在文件成功");
            }else{
                System.out.println("false删除存在文件失败");
            }

            if(fs.delete(new Path("/test02.txt"), true)){
                System.out.println("true删除存在文件成功");
            }else{
                System.out.println("true删除存在文件失败");
            }

            if(fs.delete(new Path("/3.txt"), false)){
                System.out.println("false删除不存在文件成功");
            }else{
                System.out.println("false删除不存在文件失败");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 重命名
     */
    @Test
    public void rename(){
        try {
            if(fs.rename(new Path("/tmp/1-1.sh"), new Path("/1.sh"))){
                System.out.println("rename successful...");
            } else{
                System.out.println("rename failure...");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            FSUtils.close(fs);
        }
    }

    /**
     * 创建一个空文件，并写入内容
     */
    @Test
    public void touchz(){
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = fs.create(new Path("/tmp/123.dat"));
            fsDataOutputStream.write("hello gp1707\n".getBytes());
            fsDataOutputStream.write("I am superman\n".getBytes());
            System.out.println("create and write file successful...");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fsDataOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 显示所有的文件
     */
    @Test
    public void listAllFile(){
        try {
            // 迭代器：实际上是一个指针，只有当真正访问数据的时候，才根据指针去把数据拿过来，
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);

            while(iterator.hasNext()){
                LocatedFileStatus fileStatus = iterator.next();
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getPath().getName());
                System.out.println(fileStatus.getBlockSize());
                System.out.println(fileStatus.getReplication());
                System.out.println(fileStatus.getPermission());
                System.out.println(fileStatus.getLen());

                BlockLocation[] blockLocations = fileStatus.getBlockLocations();
                for(BlockLocation blockLocation: blockLocations){
                    System.out.println("block length:"+blockLocation.getLength() +
                    "block offset:"+blockLocation.getOffset());
                    String[] hosts = blockLocation.getHosts();
                    for(String host : hosts){
                        System.out.println(host);
                    }

                    System.out.println("============================");
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            FSUtils.close(fs);
        }
    }

}
