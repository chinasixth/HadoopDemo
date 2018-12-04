package com.hmdu.bigdata.hbaseAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 10:00 2018/7/11
 * @ 创建namesapce，解决方法过时问题
 */
public class NewNamespaceDemo {
    public static void main(String[] args) throws IOException {
        // 获取配置的对象
//        Configuration conf = new Configuration();
        // 替代上面创建配置对象，这个更好
        Configuration conf = HBaseConfiguration.create();


        // 设置连接的参数
        conf.set("hbase.zookeeper.quorum", "hadoop05:2181,hadoop07:2181,hadoop08:2181");

        // 替换过时的方案
        // 获取一个连接
        Connection conn = ConnectionFactory.createConnection(conf);

        // 获取一个admin对象
        Admin admin = conn.getAdmin();

        // 获取一个namespace对象
        NamespaceDescriptor nsd = NamespaceDescriptor.create("ns3").build();

        // 提交创建namespace
        admin.createNamespace(nsd);

        // 注销对象
        admin.close();
        conn.close();

        // 打印信息，方便验证
        System.out.println("finish...");

    }

}
