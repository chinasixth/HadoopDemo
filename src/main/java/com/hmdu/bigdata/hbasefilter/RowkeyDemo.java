package com.hmdu.bigdata.hbasefilter;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:48 2018/7/12
 * @
 */
public class RowkeyDemo {
    public static void main(String[] args) {
        // 构造比较器
        RegexStringComparator rsc = new RegexStringComparator("^00001");

        // rowkey过滤器
        RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, rsc);


        // 特殊的过滤器，结果与rowfilter使用二进制前缀比较器的结果一致
        // 建议使用这个，一条语句与上面两条语句的结果是一样的
        PrefixFilter pf = new PrefixFilter(Bytes.toBytes("00001"));

        // 调用工具类中的方法，将结果打印出来

        // 打印程序结束标志
        System.out.println("finish...");
    }
}
