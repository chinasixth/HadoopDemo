package com.hmdu.bigdata.hbaseAPI.hbaseutil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 10:13 2018/7/11
 * @
 */
public class HbaseUtil {
    private static final String CONNECTION_KEY = "hbase.zookeeper.quorum";
    private static final String CONNECTION_VALUE = "hadoop05:2181,hadoop07:2181,hadoop08:2181";
    private static Logger logger = Logger.getLogger(HbaseUtil.class);

    public static Admin getAdmin() {
        Configuration conf = HBaseConfiguration.create();

        conf.set(CONNECTION_KEY, CONNECTION_VALUE);

        Admin admin = null;
        Connection conn;

        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            logger.error("连接失败...", e);
        }

        // 方法最好只留一个返回出口
        return admin;
    }

    public static void closeAdmin(Admin admin){
        // 有参数的传入要先判断参数的合法性
        if (admin == null) {
            return ;
        }
        try {
            admin.close();
        } catch (IOException e) {
            // do nothing
            // 即使将错误日志打印出来也没有用，没有办法解决
        }

    }

    public static void pringRS(){

    }


}
