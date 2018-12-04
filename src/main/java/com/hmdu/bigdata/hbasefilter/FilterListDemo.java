package com.hmdu.bigdata.hbasefilter;

import com.hmdu.bigdata.hbaseapicopy.hbaseutils.HbaseUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:29 2018/7/12
 * @ 需求：
 *   select * from t_user_info where age > 60 and sex = '1'
 *   因为有多个条件，所以需要过滤器链
 */
public class FilterListDemo {
    public static void main(String[] args) throws IOException {
        // 创建过滤器链
        // and使用pass_all
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        // 分别设置过滤条件
        // 设置age大于60的条件
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("base_info"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.LESS,
                Bytes.toBytes("60"));
        // 排除没有字段的数据，
        // 也就是在列簇中，没有age和sex列，这样的数据是不能被查询出来的
        // 默认是查询出来，所以需要手动设置将这样的数据过滤掉
        scvf.setFilterIfMissing(true);

        // 设置条件sex = 1
        SingleColumnValueFilter scvf1 = new SingleColumnValueFilter(Bytes.toBytes("base_info"),
                Bytes.toBytes("sex"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("2"));

        scvf1.setFilterIfMissing(true);

        // 将条件添加到过滤器链中
        filterList.addFilter(scvf);
        filterList.addFilter(scvf1);

        // 过滤器链是对表进行过滤的，所以要获取表管理器
        Table table = HbaseUtils.getTable("ns2:t1");

        // 创建一个scan对象，这个是用来扫描表的
        Scan scan = new Scan();

        // 讲过滤器链添加加入到scan对象，也就是在扫描的时候，同时指定条件
        scan.setFilter(filterList);

        // 开始扫描数据,扫描并返回数据
        ResultScanner scanner = table.getScanner(scan);

        // 打印结果
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            HbaseUtils.printRS(result);
        }


        // 关闭资源，table和迭代器
        HbaseUtils.closeTable(table);
        scanner.close();

        // 打印程序结束标志
        System.out.println("finish...");
    }
}
