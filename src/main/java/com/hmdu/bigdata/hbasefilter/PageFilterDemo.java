package com.hmdu.bigdata.hbasefilter;

import com.hmdu.bigdata.hbaseapicopy.hbaseutils.HbaseUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:21 2018/7/12
 * @ 需求：
 *   每页显示3行数据
 *   依次显示所有的数据
 *   1. 第一页：select * from table_name limit 3;
 *   2. 后续所有的页：select * from table_name where rowkey > max_rowkey limit 3
 *   3. 怎么判定循环结束？最后依次的行数小于3，
 *
 */
public class PageFilterDemo {
    public static void main(String[] args) throws IOException {
        // 创建分页条件，也就是每页显示多少行数据
        PageFilter pf = new PageFilter(3);

        // 获取表管理器对象
        Table table = HbaseUtils.getTable("ns2:t1");

        // 创建一个扫描器
        Scan scan = new Scan();

        // 关联pagefilter
        scan.setFilter(pf);

        // 用来记录上一次的最大rowkey
        String maxKey = "";

        // 记录判断条件
        int count = 0;

        //
        while (true) {
            // scan默认是包含开始的rowkey，所有要将比这个rowkey大才可以，这就需要使用到之前的小技巧
            // 将这条语句放在这，那么第一页和后面所有的页，查询语句的格式就变成一样的了
            scan.setStartRow(Bytes.toBytes(maxKey+ "\000") );

            count = 0;

            ResultScanner resultScanner = table.getScanner(scan);

            Iterator<Result> iterator = resultScanner.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();

                maxKey = Bytes.toString(result.getRow());
                // 使用工具类打印数据

                count ++;
            }

            if (count < 3) {
                break;
            }

        }

    }
}
