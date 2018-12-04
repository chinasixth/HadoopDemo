package com.hmdu.bigdata.hbaseAPI;

import com.hmdu.bigdata.hbaseAPI.hbaseutil.HbaseUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:28 2018/7/11
 * @
 */
public class TableDemo {
    private Admin admin = null;
    private final Logger logger = Logger.getLogger(TableDemo.class);

    @Before
    public void init() {
        admin = HbaseUtil.getAdmin();
    }

    @Test
    public void createTable() {
        // 创建表描述器
        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf("ns1:t_user_info"));

        // 创建列簇描述器，记录原来的列簇
        HColumnDescriptor hc = new HColumnDescriptor(Bytes.toBytes("extra_info"));


        // 创建表描述器，用来描述要添加的列簇
        try {
            admin.createTable(ht);
        } catch (IOException e) {
            logger.error("创建表失败", e);
        }
    }

    @Test
    public void modifyTable() {
        try {
            HTableDescriptor ht = admin.getTableDescriptor(TableName.valueOf("ns1:t_user_info"));
            HColumnDescriptor hc = ht.getFamily(Bytes.toBytes("base_info"));
            hc.setInMemory(true);
            hc.setTimeToLive(24 * 60 * 60);

            // 要增加的列簇，修改的是表
            HColumnDescriptor hc1 = new HColumnDescriptor(Bytes.toBytes("other_info"));
            hc1.setBloomFilterType(BloomType.ROW);
            hc1.setMinVersions(1);
            hc1.setMaxVersions(5);

            // 将要新增的列簇添加到表描述器中，原有的列簇虽然进行了修改，不过不用加进去
            // 因为我们获得的是对象的引用，不用添加到表描述器也达到修改的目的
            ht.addFamily(hc1);
//            ht.removeFamily();

            admin.modifyTable(TableName.valueOf("ns1:t_user_info"), ht);

        } catch (IOException e) {
            logger.error("获取表描述器失败", e);
        }
    }

    // 删除列簇
    @Test
    public void deleteColumn() {
        try {
            // 直接删除即可
            // 在表描述器中也可以删除列簇
            admin.deleteColumn(TableName.valueOf("t_user_info"), Bytes.toBytes("other_info"));
        } catch (IOException e) {
            logger.error("删除列簇失败", e);
        }
    }

    @Test
    public void deleteTable() {
        try {
            // 判断表是否存在
            if (!admin.tableExists(TableName.valueOf("t_user_info"))) {
                return;
            }

            // 判断表是否可用
            if (admin.isTableEnabled(TableName.valueOf("t_user_info"))) {
                admin.disableTable(TableName.valueOf("t_user_info"));
            }

            // 删除表之前要先禁用表
            admin.deleteTable(TableName.valueOf("t_user_info"));
        } catch (IOException e) {
            logger.error("删除表失败", e);
        }
    }

    @After
    public void close() {
        HbaseUtil.closeAdmin(admin);
    }


//    public static void main(String[] args) {
//        Admin admin = HbaseUtil.getAdmin();
//
//        // 创建表描述器,这样直接创建一个表描述器，得到的表的列簇，只有新增的
//        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf("ns1:userinfo"));
//        // 要想获得以前的列簇和新增的列簇，就要先获得以前的表描述器，然后新增一个列簇
//
//
//
//        // 创建一个列簇描述器
//        HColumnDescriptor hc = new HColumnDescriptor(Bytes.toBytes("base_info"));
//        hc.setBloomFilterType(BloomType.ROW);
//        hc.setVersions(1, 5);
//        hc.setTimeToLive(24*60*60);
//
//        // 给表增加一个列簇,将列簇添加到表描述器
//        ht.addFamily(hc);
//        try {
//            admin.createTable(ht);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("finish...");
//    }
}
