package com.hmdu.bigdata.hbaseAPI;

import com.hmdu.bigdata.hbaseAPI.hbaseutil.HbaseUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 10:11 2018/7/11
 * @
 */
public class NamespaceDemo {
    private Admin admin = null;
    private Logger logger = Logger.getLogger(NamespaceDemo.class);

    @Before
    public void init() {
        admin = HbaseUtil.getAdmin();
    }

    @Test
    public void listNamespace() {
        try {
            // 获取namespace描述器的列表
            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();

            // 循环打印namespace描述器的信息
            for (NamespaceDescriptor nsd : namespaceDescriptors) {
                System.out.println(nsd);
            }

            System.out.println("finish...");
        } catch (IOException e) {
            logger.error("列举nemespace失败", e);
        }

    }

    @Test
    public void listTables() {
        // 列举某个namespace下的所有表
        try {
            // 返回所有namespace下的table
            HTableDescriptor[] hTableDescriptors = admin.listTables();

            for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
                System.out.println(hTableDescriptor.getNameAsString());
            }
        } catch (IOException e) {
            logger.error("列举namespace下的表失败", e);
        }
    }

    @Test
    public void listNamespaceTables() {
        // 列举指定namespace下的表
        try {
            HTableDescriptor[] ns1s = admin.listTableDescriptorsByNamespace("ns1");

            for (HTableDescriptor ns1 : ns1s) {
                System.out.println(ns1.getNameAsString());
            }
        } catch (IOException e) {
            logger.error("列举某个namesapce下的表失败", e);
        }
    }

    @Test
    public void DeleteNamespace() {

        // 先判断namespae是否为null
        try {
            HTableDescriptor[] ns1s = admin.listTableDescriptorsByNamespace("ns1");
            if (ns1s.length > 0) {
                System.out.println("namespace:ns1不为空，请先删除ns1下的表");
                return;
            }
        } catch (IOException e) {
            logger.error("列举namespace下的表失败", e);
        }

        try {
            admin.deleteNamespace("ns1");
        } catch (IOException e) {
            logger.error("删除namesapce失败", e);
        }
    }


    // 当整个Test执行完以后，都会执行这个after
    // 注意：测试方法不能加上static
    @After
    public void close() {
        HbaseUtil.closeAdmin(admin);
    }
}
