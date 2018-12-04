package com.hmdu.bigdata.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:55 2018/7/2
 * @ 注意：
 * 1.自定义partitioner类需要继承Partitioner抽象类
 * 2.分区的类型要和map的输出类型(reduce输入类型)相一致
 * 3.要实现getPartitione方法，并且方法的返回值类型只能是int类型
 * 4.分区的数量尽量跟reduce的数量相等，
 * 5.分区的返回值尽量使用%，这样可以避免分区输出数大于reduce的数量
 * 6.默认使用的是HashPartitioner
 */
public class MyPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        // 判断数据属于哪个分区
        String first = key.toString();

        if (first.matches("^[a-z]")) {
            return 0 % numPartitions;
        } else if (first.matches("^[A-Z]")) {
            return 1 % numPartitions;
        } else if (first.matches("^[0-9]")) {
            return 2 % numPartitions;
        } else {
            return 3 % numPartitions;
        }

    }
}
