package com.hmdu.bigdata.hbaseAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:33 2018/7/11
 * @ 创建一个namespace
 */
public class CreateNamespaceDemo {
    public static void main(String[] args) throws IOException {
        // 1.创建configuration对象
        Configuration conf = new Configuration();

        // 2.设置连接参数
        conf.set("hbase.zookeeper.quorum", "hadoop05:2181,hadoop07:2181,hadoop08:2181");

        // 3.获取一个admin对象，从客户端操作hbase
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);

        // 4.获取一个namesapce的描述器,hbase中有两个默尔的namesapce，
        //   根据这个可以创建namespace的描述器
        NamespaceDescriptor nsd = NamespaceDescriptor.create("ns2").build();

        // 5.提交并创建namespace
        hBaseAdmin.createNamespace(nsd);

        // 6.销毁对象
        hBaseAdmin.close();

        // 7.打印结束标志，方便测试
        System.out.println("finish...");
    }


}
