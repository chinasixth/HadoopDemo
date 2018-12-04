package com.hmdu.bigdata.hbasefilter;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 10:56 2018/7/12
 * @
 */
public class FamilyFilterDemo {
    public static void main(String[] args) {
        // 正则比较器
        RegexStringComparator rsc = new RegexStringComparator("^bas");

        // 字串比较器
        SubstringComparator sc = new SubstringComparator("info");

        // 二进制前缀比较器，这种前缀的东西，会使用索引，所以以后还是经常使用这种吧
        BinaryPrefixComparator bpc = new BinaryPrefixComparator(Bytes.toBytes("ba"));

        // 二进制比较器
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("base_info"));

        // 列簇比较器,将各种条件比较器注入到列簇比较器中
        // 将整个列簇中的数据查询出来
        // 返回值：只返回满足列簇比较器条件的列簇中的数据
//        FamilyFilter ff = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
//                rsc);

        FamilyFilter ff = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                sc);

        //
        PrefixFilter pf = new PrefixFilter(Bytes.toBytes("base"));


        // 调用工具类中的测试方法

        // 打印程序结束信息
        System.out.println("finish...");
    }
}













