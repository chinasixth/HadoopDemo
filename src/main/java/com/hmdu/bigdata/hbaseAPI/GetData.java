package com.hmdu.bigdata.hbaseAPI;

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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 15:59 2018/7/11
 * @
 */
public class GetData {
    private final Logger logger = Logger.getLogger(GetData.class);
    private Table table = null;

    @Before
    public void init() {


    }

    @Test
    public void getData() {
        // 获取到get对象,这个对象可以根据指定的rowkey，得到所有的数据
        Get get = new Get(Bytes.toBytes("00001"));
        try {
            Result result = table.get(get);
            NavigableMap<byte[], byte[]> maps = result.getFamilyMap(Bytes.toBytes("base_info"));

            for (Map.Entry<byte[], byte[]> map : maps.entrySet()) {
                System.out.println(Bytes.toString(map.getKey()) + ":" + Bytes.toString(map.getValue()));
            }

            System.out.println();
        } catch (IOException e) {
            logger.error("查询数据失败", e);
        }
    }

    @Test
    public void getDataWithColumn() {
        // 获取Get对象
        Get get = new Get(Bytes.toBytes("00001"));

        // 增加一个，按列簇增加数据
        get.addFamily(Bytes.toBytes("base_info"));

        try {
            Result result = table.get(get);

            CellScanner cellScanner = result.cellScanner();

            System.out.println(Bytes.toString(result.getRow()));

            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                System.out.print("\t" + new String(CellUtil.cloneRow(cell), "utf-8")
                        + "\t" + new String(CellUtil.cloneFamily(cell), "utf-8")
                        + ":" + new String(CellUtil.cloneQualifier(cell), "utf-8")
                        + "-" + new String(CellUtil.cloneValue(cell), "utf-8"));
            }

            System.out.println();

        } catch (IOException e) {
            logger.error("获取数据失败", e);
        }
    }

    @Test
    public void getListData() {
        ArrayList<Get> gets = new ArrayList<>();
        Get get = new Get(Bytes.toBytes("00001"));

        // 可以往get中多增加几个rowkey
        // get.add(get1);

        try {
            Result[] results = table.get(gets);
            for (Result result : results) {
                //
            }

        } catch (IOException e) {
            logger.error("获取数据失败", e);
        }
    }

    // scan操作
    @Test
    public void scanTable() {
        // new一个scan对象
        Scan scan = new Scan();

        // 扫描的结果是前开后闭区间
        scan.setStartRow(Bytes.toBytes(""));
        scan.setStopRow(Bytes.toBytes(""));
        // 如果只想获得某一个具体的rowkey，以使用下面的小技巧
        scan.setStartRow(Bytes.toBytes("00001" + "\000"));
        scan.setStopRow(Bytes.toBytes("00001" + "\000"));

        try {
            ResultScanner scanner = table.getScanner(scan);

            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();

                // 打印

            }

        } catch (IOException e) {
            logger.error("扫描表失败", e);
        }

    }

    @Test
    public void deleteData(){
        // new一个delete对象
        Delete delete = new Delete(Bytes.toBytes("00001"));

        // 设置删除的条件
        delete.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"));

        // 可以将多个已经设置好删除条件的delete对象组成一个集合
        ArrayList<Delete> dels = new ArrayList<>();


        try {
            // 提交删除
            // 注意：这样的删除只会将最新版本号的数据删除掉，低版本号的数据就会暴漏出来
            // 如果是删除整行的话，那么就不会出现版本号的问题了
            table.delete(delete);
            table.delete(dels);

        } catch (IOException e) {
            logger.error("删除失败", e);
        }
    }





    @After
    public void close() {
        try {
            table.close();
        } catch (IOException e) {
            //
        }
    }
}
