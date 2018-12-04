package com.hmdu.bigdata.hbaseapicopy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 19:10 2018/7/11
 * @ 通过JavaAPI创建Hbase中的namespace
 * namespace在hbase中相当于是数据库中的库的概念，利用面向对象的编程思想，我们可以将namespace看成是一个对象
 * 我们要想操作hbase中的对象，首先肯定是通过配置对象获取一个操作hbase的对象
 * 首先，获取一个hbase的配置对象，然后对这个配置对象设置连接参数
 * 通过配置对象来获取和hbase的连接对象，为什么不能直接获得一个操作namespace的操作对象，因为hbase中的表，列簇等都可以看成是一个对象
 * 我们操作的级别不一样，不能通过一个客户端去操作各个级别的东西
 * 在JavaAPI中操作hbase中的namespace、table、列簇等，都需要有一个它们对应的描述器，
 * 然后通过描述器来对它们进行操作，然后在通过操作它们的客户端将描述器注入，真正的写入到hbase中
 * 这里要注意：如果描述器是由客户端从hbase中获取的，可以不用再次注入客户端，因为获取的描述器对象是引用
 *
 *
 */
public class CreateNamespaceDemoCopy {
    private static String CONNECTION_KEY = "hbase.zookeeper.quorum";
    private static String CONNECTION_VALUE = "hadoop05:2181,hadoop07:2181,hadoop08:2181";

    public static void main(String[] args) throws IOException {
        // 首先获得一个有关配置属性的对象
        Configuration conf = HBaseConfiguration.create();

        // 设置连接参数
        conf.set(CONNECTION_KEY, CONNECTION_VALUE);

        // 有了配置对象，接下来就要拿着这个配置对象去连接hbase
        Connection conn = ConnectionFactory.createConnection(conf);

        // 要先操作hbase，就要先获得一个客户端的对象
        // 有了连接，用这个连接去获得一个客户端对象
        Admin admin = conn.getAdmin();

        // 有了客户端对象，就可以创建一个namespace，
        // 要想在hbase中创建一个namespace，就要先创建一个用来描述namespace的对象，设置好namespace的各种属性
        // 也就是创建一个namespace的描述器对象
        // 然后将这个namespace通过客户端放到hbase中
        // 获取namespace的描述器对象
        NamespaceDescriptor nsd = NamespaceDescriptor.create("ns3").build();

        // 提交namespace描述器对象，创建namespace
        admin.createNamespace(nsd);

        // 打印信息，方便查看程序是否执行完毕
        System.out.println("finish...");

    }
}
