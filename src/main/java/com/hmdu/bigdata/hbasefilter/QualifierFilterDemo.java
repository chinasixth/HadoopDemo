package com.hmdu.bigdata.hbasefilter;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:15 2018/7/12
 * @ 列名过滤器：返回结果中只包含满足条件的列
 */
public class QualifierFilterDemo {
    public static void main(String[] args) {
        RegexStringComparator rsc = new RegexStringComparator("^na");

        // 字串比较器

        // 前缀比较器

        // 二进制前缀比较器

        QualifierFilter qf = new QualifierFilter(CompareFilter.CompareOp.EQUAL, rsc);

        // 作用在列名上的列名前缀比较器，是一种特殊的比较过滤器，
        // 本质上是：列名过滤器使用二进制前缀比较器的特殊用法
        // 使用时直接指定列名，方便快捷，相当于上面的两条语句
        ColumnPrefixFilter cpf = new ColumnPrefixFilter(Bytes.toBytes("na"));

        // 多列名过滤器
        byte[][] prefixs = new byte[][]{Bytes.toBytes("na"),
            Bytes.toBytes("ag")};

        // 返回值的结果，只要满足一个添加就可以
        MultipleColumnPrefixFilter mcpf = new MultipleColumnPrefixFilter(prefixs);

        // 列名范围过滤器
        // 第二和第四个参数的作用：是否包含最小列和最大列，按照字典顺序来比较的
        ColumnRangeFilter crf = new ColumnRangeFilter(Bytes.toBytes("name"), true, Bytes.toBytes("age"), false);

        // 调用工具类中的测试方法

        // 打印程序结束标志
        System.out.println("finish...");
    }
}
