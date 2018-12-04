package com.hmdu.bigdata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:23 2018/6/28
 */
/*
 * 查找出每一年最高的温度
 *
 *
 * */
public class HighTemperature {
    public static Text k = new Text();
    public static DoubleWritable v = new DoubleWritable();
    public static Double max = Double.MIN_VALUE;

    public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1.数据类型转换
            String line = value.toString();

            // 2. 截取年份和温度
            String year = line.substring(0, 4);
            Double temperature = Double.valueOf(line.substring(8));

            // 此处为优化
            k.set(year);
            v.set(temperature);

            // 3.将结果输出
            context.write(k, v);

        }
    }

    public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            // 1.遍历迭代器，寻找最大值
            for (DoubleWritable temperature : values) {
                if (max < temperature.get()){
                    max = temperature.get();
                }
            }

            // 2.输出结果
            v.set(max);
            context.write(key, v);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取配置对象
        Configuration conf = new Configuration();

        // 2.创建一个Job对象
        Job job = Job.getInstance(conf, "high temperature");

        // 3.指定job的jar执行路径
        job.setJarByClass(HighTemperature.class);

        // 4.指定本job的map业务类以及map方法的输出类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // 5.指定本job的reduce业务类以及reduce方法的输出类型
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // 6.指定本job的输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 7.指定本job的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8.提交job
        boolean isok = job.waitForCompletion(true);

        // 9.退出
        System.exit(isok ? 0 : 1);


    }

}

