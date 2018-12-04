package com.hmdu.bigdata.hbasefilter;

import com.hmdu.bigdata.hbaseapicopy.hbaseutils.HbaseUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:59 2018/7/12
 * @ 主要目的用来计数
 */
public class FirstKeyOnlyFilterDemo {
    public static void main(String[] args) {
        // 这个并不是将count的结果返回
        FirstKeyOnlyFilter fkof = new FirstKeyOnlyFilter();

        //
        Table table = HbaseUtils.getTable("ns2:t1");

        Scan scan = new Scan();

        scan.setFilter(fkof);

        try {
            ResultScanner scanner = table.getScanner(scan);

            int count = 0;

            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }

            System.out.println("总行数为：" + count);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
