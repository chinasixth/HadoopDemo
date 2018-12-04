package com.hmdu.bigdata.mapreduceapi;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 20:27 2018/6/24
 */
public class MyPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String firstChar = key.toString().substring(0, 1);
        if(firstChar.matches("^[A-Z]]")){
            return 0 % numPartitions;
        } else if (firstChar.matches("^[a-z]")){
            return 1 % numPartitions;
        } else if (firstChar.matches("^[1-9]")){
            return 2 % numPartitions;
        }else{
            return 3 % numPartitions;
        }
    }
}
