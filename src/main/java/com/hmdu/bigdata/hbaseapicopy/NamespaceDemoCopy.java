package com.hmdu.bigdata.hbaseapicopy;

import com.hmdu.bigdata.hbaseapicopy.hbaseutils.HbaseUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 19:47 2018/7/11
 * @ JavaAPI对namespace进行删除和查看
 */
public class NamespaceDemoCopy {
    private Admin admin = null;
    private final Logger logger = Logger.getLogger(NamespaceDemoCopy.class);

    @Before
    public void init() {
        admin = HbaseUtils.getAdmin();
    }

    // 列出所有的namespace
    /*
    * 想要列出所有的namespace，就需要通过admin客户端获得所有的namespace的描述器
    * 这些描述器中就存放着namespace的信息，如：namespace的name
    * */
    @Test
    public void listNamespace() {
        try {
            // 通过连接到namespace的客户端，获得所有的namespace的描述器
            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();

            // 根据这些namespace的描述器，将所有的namespace信息打印出来
            for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
                System.out.println("name\t" + namespaceDescriptor.getName()
                        + "\ttoString" + namespaceDescriptor.toString());
            }

            System.out.println("finish...");
        } catch (IOException e) {
            logger.error("获取namespace失败", e);
        }
    }

    /*
    * 列出指定的namespace下的所有的table名称
    * 注意：这里获取到的并不是表描述器，而是TableName类型的数组
    * */
    @Test
    public void listTables() {
        try {
            TableName[] tables = admin.listTableNamesByNamespace("ns1");

            for (TableName table : tables) {
                System.out.println("tableName:" + table.getNameAsString()
                        + "\tnamespace:" + new String(table.getNamespace()));
            }

            System.out.println("finish...");
        } catch (IOException e) {
            logger.error("列举指定namespace的所有表失败", e);
        }
    }

    /*
    * 列举出hbase中所有的表
    * 通过客户端获取所有表的表描述器
    * */
    @Test
    public void listAllTables() {
        try {
            HTableDescriptor[] hts = admin.listTables();

            for (HTableDescriptor ht : hts) {
                System.out.println("tablename:" + ht.getTableName());
            }
            System.out.println("finish...");
        } catch (IOException e) {
            logger.error("列举所有的表失败", e);
        }
    }

    /*
    * 使用admin客户端删除指定名称的namespace
    * */
    @Test
    public void deleteNamespace() {
        try {
            admin.deleteNamespace("ns3");

            System.out.println("删除namespace成功");
        } catch (IOException e) {
            logger.error("删除namespace失败", e);
        }
    }

    @After
    public void close() {
        HbaseUtils.closeAdmin(admin);
    }
}
