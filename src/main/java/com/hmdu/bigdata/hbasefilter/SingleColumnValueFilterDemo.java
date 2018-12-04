package com.hmdu.bigdata.hbasefilter;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 10:06 2018/7/12
 * @ 单值过滤器
 */
public class SingleColumnValueFilterDemo {
    public static void main(String[] args) {
        // 正则比较器，这个是可以使用索引的
        RegexStringComparator rsc = new RegexStringComparator("^[zhang]");

        // 字串比较器，不能使用索引
        SubstringComparator sc = new SubstringComparator("yuan");

        // 二进制前缀比较器
        BinaryPrefixComparator bpc = new BinaryPrefixComparator(Bytes.toBytes("gao"));

        // 二进制比较器
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("zs01"));


        // 构造单值比较器对象
//        SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("ns2:t1"),
//                Bytes.toBytes("name"),
//                CompareFilter.CompareOp.GREATER,
//                Bytes.toBytes("zs"));

        // 注意：使用正则比较器，第三个参数要设置为相等或不相等
        // 如果数据没有正则比较器指定的列，默认是将这个数据拿出来的
//        SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("ns2:t1"),
//                Bytes.toBytes("name"),
//                CompareFilter.CompareOp.EQUAL,
//                rsc);

        // 使用字串比较器
//        SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("ns2:t1"),
//                Bytes.toBytes("name"),
//                CompareFilter.CompareOp.EQUAL,
//                sc);

        // 使用前缀比较器
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("ns2:t1"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                bpc);

        // 使用二进制比较器

        scvf.setFilterIfMissing(true);

        // 调用测试方法，方法已经被封装

        // 打印程序结束标志
        System.out.println("finish...");
    }
}
