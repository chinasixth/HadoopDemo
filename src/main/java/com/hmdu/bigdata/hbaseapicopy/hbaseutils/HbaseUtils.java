package com.hmdu.bigdata.hbaseapicopy.hbaseutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 19:35 2018/7/11
 * @
 */
public class HbaseUtils {
    private static final String CONNECTION_KEY = "hbase.zookeeper.quorum";
    private static final String CONNECTION_VALUE = "hadoop05:2181,hadoop07:2181,hadoop08:2181";

    private static final Logger logger = Logger.getLogger(HbaseUtils.class);

    private static Admin admin = null;

    private static Connection conn = null;

    static {
        Configuration conf = new Configuration();

        conf.set(CONNECTION_KEY, CONNECTION_VALUE);

        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            logger.error("创建连接失败", e);
        }
    }

    public static void closeConn(){
        if(conn == null){
            return ;
        }
        try {
            conn.close();
        } catch (IOException e) {
            // do nothing
        }
    }


    public static Admin getAdmin() {

        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            logger.error("创建hbase客户端失败", e);
        }
        return admin;
    }

    public static void closeAdmin(Admin admin) {
        if (admin == null) {
            return;
        }
        try {
            admin.close();
        } catch (IOException e) {
            //do nothing
        }
    }

    /*
     * 获取表管理器，下面两个重载的方法，如果没有给定参数，就获取默认的表的管理器
     * 如果给定了参数，直接获取参数所指定的表
     * */
    public static void getTable() {
        getTable("ns2:t1");
    }

    /*
     * 获取表的指定表名的操作对象，即表管理器
     * 获取这个对象是使用hbase的连接，并根据指定tablename的TableName类型来获取的
     * */
    public static Table getTable(String tableName) {
        // 要将string类型的tableName转换成TableName类型的
        TableName tName = TableName.valueOf(tableName);
        Table table = null;

        try {
            table = conn.getTable(tName);
        } catch (IOException e) {
            logger.error("获取表管理器失败", e);
        }

        return table;
    }

    public static void closeTable(Table table) {
        if (table == null) {
            return;
        }
        try {
            table.close();
        } catch (IOException e) {
            //
        }
    }

    public static void printRS(Result result) {
        CellScanner cellScanner = result.cellScanner();

        System.out.print(Bytes.toString(result.getRow()));

        try {
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                System.out.print("\t" + new String(CellUtil.cloneFamily(cell), "utf-8")
                        + " : " + new String(CellUtil.cloneQualifier(cell), "utf-8")
                        + " = " + new String(CellUtil.cloneValue(cell), "utf-8"));
            }
            System.out.println();

        } catch (IOException e) {
            logger.error("判断是否有下一个单元格失败！", e);
        }
    }

    public static void printRS(ResultScanner resultScanner) {
        Iterator<Result> iterator = resultScanner.iterator();

        while (iterator.hasNext()) {
            Result result = iterator.next();
            printRS(result);
        }
    }

    public static void scanTest(Filter filter) {
        scanTest("ns2:t1", filter);
    }

    public static void scanTest(String tableName, Filter filter) {
        Table table = HbaseUtils.getTable(tableName);

        // 创建一个scan对象，这个是用来扫描表的
        Scan scan = new Scan();

        // 讲过滤器链添加加入到scan对象，也就是在扫描的时候，同时指定条件
        scan.setFilter(filter);

        // 开始扫描数据,扫描并返回数据
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);

            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                HbaseUtils.printRS(result);
            }
        } catch (IOException e) {
            logger.error("扫描数据失败", e);
        } finally {
            HbaseUtils.closeTable(table);
            scanner.close();
        }

        System.out.println("finish...");
    }
}
