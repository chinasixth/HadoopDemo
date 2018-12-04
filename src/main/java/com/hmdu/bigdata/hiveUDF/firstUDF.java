package com.hmdu.bigdata.hiveUDF;

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 21:13 2018/7/6
 * @ 使用UDF（user-defined function）测试
 * hive中有三种UDF：普通UDF：
 * 普通的UDF操作作用于单个数据行，并且产生一个数据行作为输出
 * UDAF：接收多个数据数据行，并产生一个输出数据行
 * UDTF：用于操作单个数据行，并产生多个数据行---->也就是将一个表作为输出
 *
 * 我们写的UDF必须是继承了UDF的，并且实现evaluate()方法，这是必须的
 */
public class firstUDF extends UDF {
    public static String evaluate(String str) {
        if (Strings.isEmpty(str)) {
            return null;
        }

        return Strings.toLowerCase(str);
    }
}
