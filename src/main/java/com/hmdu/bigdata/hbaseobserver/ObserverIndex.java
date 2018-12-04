package com.hmdu.bigdata.hbaseobserver;

import com.hmdu.bigdata.hbaseapicopy.hbaseutils.HbaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 10:07 2018/7/13
 * @ 需求：
 * A-B f1:from A
 * A-B f1:to B
 * 在另一张表中写入
 * B-A f1:to A
 * B-A f1:from B
 */
public class ObserverIndex extends BaseRegionObserver {
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
                       Put put, WALEdit edit,
                       Durability durability)
            throws IOException {
        // 获取rowkey
        String rowkey = Bytes.toString(put.getRow());

        boolean flag = false;

        // 反转rowkey
        String[] split = rowkey.split("-");
        String newRowkey = split[1] + "-" + split[0];
        Put newPut = new Put(Bytes.toBytes(newRowkey));

        CellScanner cellScanner = put.cellScanner();

        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();

            // 获取要写入的列簇
            String columnFamily = new String(CellUtil.cloneFamily(cell));

            // 获取列名
            String column = new String(CellUtil.cloneQualifier(cell));

            // 获取值
            String value = new String(CellUtil.cloneValue(cell));

            // 只处理列簇为f1的数据
            if (columnFamily.equals("f1")) {
                // 判断
                if (column.equals("from")) {
                    newPut.addColumn(Bytes.toBytes(columnFamily),
                            Bytes.toBytes("to"),
                            Bytes.toBytes(value));

                    flag = true;
                } else if (column.equals("to")) {
                    newPut.addColumn(Bytes.toBytes(columnFamily),
                            Bytes.toBytes("from"),
                            Bytes.toBytes(value));
                    flag = true;
                }
            }
        }

        // 如果修改值了，就使用表管理器提交数据
        if (flag) {
            // 获取表管理器
            Table table = HbaseUtils.getTable("ns2:t_fensi");

            // 提交数据
            table.put(newPut);
        }
        super.prePut(e, put, edit, durability);
    }
}
