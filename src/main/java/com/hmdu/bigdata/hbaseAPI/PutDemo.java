package com.hmdu.bigdata.hbaseAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:44 2018/7/11
 * @
 */
public class PutDemo {
    private static Logger logger = Logger.getLogger(PutDemo.class);


    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper", "hadoop05:2181,hadoop07:2181,hadoop08:2181");

        Connection conn = ConnectionFactory.createConnection(conf);

        // 获取一个表管理器，使用下面的没有过期的
        HTable hTable = new HTable(conf, TableName.valueOf("t_user_info"));
//        Table table = conn.getTable(TableName.valueOf("t_user_info"));

        // 构造一个put
        Put put = new Put(Bytes.toBytes("00001"));
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("sex"), Bytes.toBytes("2"));

        // 提交数据
        hTable.put(put);
    }

    //
    public static Table getTable(){
        return getTable("ns1:t_user_info");
    }

    public static Table getTable(String table){
        TableName tn = TableName.valueOf(table);
        return null;
    }
}
