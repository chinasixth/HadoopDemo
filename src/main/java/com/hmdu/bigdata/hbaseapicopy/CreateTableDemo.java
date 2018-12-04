package com.hmdu.bigdata.hbaseapicopy;

import com.hmdu.bigdata.hbaseapicopy.hbaseutils.HbaseUtils;
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
 * @ Date   ：Created in 20:15 2018/7/11
 * @ 对hbase中表的操作
 */
public class CreateTableDemo {
    private Admin admin = null;
    private final Logger logger = Logger.getLogger(NamespaceDemoCopy.class);

    @Before
    public void init() {
        admin = HbaseUtils.getAdmin();
    }

    @Test
    public void createTable() {

        // 要想在namespace中创建一个表，就要先创建一个表的描述器，然后用表的描述器来设置表的各种属性
        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf("ns2:t1"));

        // 创建一个列簇描述器
        HColumnDescriptor hc = new HColumnDescriptor(Bytes.toBytes("extra_info"));

        // 如果在创建表的时候同时指定列簇，那么在此之前就需要创建一个列簇描述器，
        // 然后将这个列簇描述器添加到表描述器中
        ht.addFamily(hc);

        // 通过客户端创建一个表，传入的参数是一个表的描述器，那么在此之前就要先创建出一个表描述器，并设置好表的各种属性
        try {
            admin.createTable(ht);

            System.out.println("finish...");
        } catch (IOException e) {
            logger.error("创建表失败", e);
        }
    }

    /*
     * 修改表包括增加列簇、删除列簇、修改布隆过滤器、修改版本号等
     * */
    @Test
    public void modifyTable() {
        // 修改原来的列簇，需要先获取原来列簇的描述器，然后通过描述器进行修改，
        // 注意：这里获得列簇描述器修改完以后，可以不用admin再写入到表中，
        // 因为对象操作使用的是对象的引用，所以在表描述器中修改，表中的内容也会跟着修改，所以不用再次写入

        // 要想获得列簇的描述器，就要先获得表的描述器，因为列簇是表的一部分嘛
        // 通过客户端连接获得表的描述器
        try {
            HTableDescriptor ht = admin.getTableDescriptor(TableName.valueOf("ns2:t1"));

            // 利用表的描述器获得列簇的描述器,这里可以获得所有的列簇的描述器，也可以获得指定的列簇描述器
            HColumnDescriptor hc = ht.getFamily(Bytes.toBytes("extra_info"));

            // 接下来可以根据hc去设置列簇的各个属性值
            hc.setBloomFilterType(BloomType.ROWCOL);

            // 接下来就是自己创建一个需要新增的列簇的列簇描述器
            HColumnDescriptor hc1 = new HColumnDescriptor(Bytes.toBytes("base_info"));

            hc1.setTimeToLive(12 * 24 * 60 * 60);

            // 将设置好的列簇描述器添加到表描述器中
            // 这里要注意，如果表描述器不是从客户端连接获得的，而是由自己创建的，那么将这个表描述器添加到hbase后，
            // 原来的列簇就会消失，只会显示新增的列簇，也就是只会显示刚才添加的表描述器中有的列簇
            ht.addFamily(hc1);

            // 提交表描述器，修改表。增删列簇等，都属于对表的修改，所以使用modifyTable
            admin.modifyTable(TableName.valueOf("ns2:t1"), ht);

            System.out.println("finish...");
        } catch (IOException e) {
            logger.error("获取表描述器失败", e);
        }
    }


    @Test
    public void deleteFamily() {
        try {
            admin.deleteColumn(TableName.valueOf("ns2:t1"), Bytes.toBytes("other_info"));

            System.out.println("finish...");
        } catch (IOException e) {
            logger.error("删除列簇失败", e);
        }
    }

    @Test
    public void deleteTable() {
        // 这里要注意：
        // 我们在删除表的时候，首先要判断一下这个表是否存在，
        // 当表存在时，我们要判断表是否被禁用，因为我们只能删除被禁用的表
        try {
            if (!admin.tableExists(TableName.valueOf("ns2:t1"))) {
                return;
            }

            if (admin.isTableEnabled(TableName.valueOf("ns2:t1"))) {
                admin.disableTable(TableName.valueOf("ns2:t1"));
            }

            admin.deleteTable(TableName.valueOf("ns2:t1"));

            System.out.println("finish...");
        } catch (IOException e) {
            logger.error("删除表失败", e);
        }
    }


    @After
    public void close() {
        HbaseUtils.closeAdmin(admin);
    }
}
