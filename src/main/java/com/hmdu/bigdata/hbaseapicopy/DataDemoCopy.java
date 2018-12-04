package com.hmdu.bigdata.hbaseapicopy;

import com.hmdu.bigdata.hbaseapicopy.hbaseutils.HbaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 8:42 2018/7/12
 * @
 */
public class DataDemoCopy {
    private static String CONNECTION_KEY = "hbase.zookeeper.quorum";
    private static String CONNECTION_VALUE = "hadoop05:2181,hadoop07:2181,hadoop08:2181";

    private static final Logger logger = Logger.getLogger(DataDemoCopy.class);

    private Table table = null;

    @Before
    public void init() {
        table = HbaseUtils.getTable("ns2:t1");
    }
    /*
     * 要想对表中的数据进行操作，就要先获得表管理器，然后通过表管理器对表中的数据进行操作
     * 然而，获得表管理器，就需要先连接到hbase，从这个连接中获取表管理器，
     * 连接到hbase需要配置信息。
     * 通过以上分析，好像每一个步骤都需要获取配置对象，根据配置对象连接到hbase，
     * 既然如此，那么就整一个工具类中的静态方法好了，这个静态方法返回值就是表管理器
     * */

    @Test
    public void putData() throws IOException {
        // 使用表管理器向表中写入数据，就要先构造一个对数据的描述器
        // 构造对数据的描述器的时候，要指定rowkey
        Put put = new Put(Bytes.toBytes("00002"));

        // 对数据进行设置，比如指定列簇、指定列名和对应的数据，
        put.addColumn(Bytes.toBytes("base_info"),
                Bytes.toBytes("sex"),
                Bytes.toBytes("2"));

        // 将数据描述器添加到表描述器中
        table.put(put);

        System.out.println("finish...");
    }

    /*
     * 获取表中的数据用的是get对象，这是一个读数据的对象，和put的方向相反
     * get对象是直接获得，通过传入的参数，也就是指定rowkey，就可以获取数据了
     * 当然，要想真的获取到数据，就要把这个get对象注入到表对象中
     * */
    // 这是按照rowkey获取数据
    @Test
    public void getData() {
        // 创建get对象，并指定rowkey
        Get get = new Get(Bytes.toBytes("00001"));

        // 通过table来注入get，将数据结果返回
        try {
            Result result = table.get(get);

            // 获取的数据是一个cell，也就是一个单元格，
            // 一个单元格中的数据是rowkey和列簇交叉的部分
            // 也就是可能有很多的列簇，如此，查看数据，就要在返回的数据结果中指定要查看的是哪个列簇的数据
            NavigableMap<byte[], byte[]> maps = result.getFamilyMap(Bytes.toBytes("base_info"));

            // 这个map中就是列与值的对应
            // 将maps中的数据循环遍历出来
            for (Map.Entry<byte[], byte[]> entry : maps.entrySet()) {
                // 上面选择的是entryset，是将众多的map项按照横向切割
                // 将数据打印出来
                System.out.print(Bytes.toString(entry.getKey()) + ":"
                        + Bytes.toString(entry.getValue())
                        + "\t");
            }

            System.out.println();
        } catch (IOException e) {
            logger.error("获取数据失败", e);
        }
    }

    /*
     * 根据列簇获取数据
     * */
    @Test
    public void getDataByColumn() {
        Get get = new Get(Bytes.toBytes("00001"));
        // 根据列簇取数据
//        get.addFamily(Bytes.toBytes("base_info"));
        get.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"));
        get.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("pic"));

        try {
            Result result = table.get(get);

            CellScanner cellScanner = result.cellScanner();

            System.out.print(Bytes.toString(result.getRow()) + "\t");
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();

                System.out.print(new String(CellUtil.cloneFamily(cell), "utf-8") + "\t"
                        + new String(CellUtil.cloneQualifier(cell), "utf-8") + "\t"
                        + new String(CellUtil.cloneValue(cell), "utf-8"));
            }

            System.out.println();
        } catch (IOException e) {
            logger.error("获取数据失败", e);
        }
    }


    /*
     * 普通的按行查询，scan指定起始rowkey和结束rowkey，不包括最后一个rowkey*/
    @Test
    public void scanData() {
        // 获取一个scan对象
        Scan scan = new Scan();

        // 设置scan对象，也就是设置要查询的条件
        scan.setStartRow(Bytes.toBytes("00001"));

        scan.setStopRow(Bytes.toBytes("00002" + "\000"));

        try {
            ResultScanner resultScanner = table.getScanner(scan);

            Iterator<Result> iterator = resultScanner.iterator();

            while (iterator.hasNext()) {
                Result result = iterator.next();

                HbaseUtils.printRS(result);
            }

            resultScanner.close();
        } catch (IOException e) {
            logger.error("scan表中的数据失败", e);
        }
    }

    @Test
    public void deleteData() {
        /*
         * 要想删除数据，就要构造一个delete对象，然后注入到table中
         * */
        Delete del = new Delete(Bytes.toBytes("0000"));

        // 可以继续为delete对象指定更多的删除条件，比如指定删除哪一个列簇中的哪一个列

        try {
            table.delete(del);
        } catch (IOException e) {
            logger.error("删除数据失败", e);
        }
    }


    @After
    public void close() {
        HbaseUtils.closeTable(table);
    }

}















