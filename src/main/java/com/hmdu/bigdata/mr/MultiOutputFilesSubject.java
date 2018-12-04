package com.hmdu.bigdata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 19:16 2018/7/2
 * @ 这个类中仅仅是说明两个知识点，此类不能运行
 */
public class MultiOutputFilesSubject {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private static String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 获得切片
            InputSplit inputSplit = context.getInputSplit();

            // 转换成文件切片，切片中包含很多的信息，比如文件的路径、长度等
            FileSplit fileSplit = (FileSplit) inputSplit;

            // 获取文件的名字
            fileName = fileSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 写map方法中的业务逻辑
            super.map(key, value, context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        private MultipleOutputs<Text, LongWritable> multipleOutputs = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            // 业务逻辑，然后发往不同的文件中
            multipleOutputs.write("文件名", key, values.iterator().next().get());
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "jlkj");

        // 设置多文件输出
        MultipleOutputs.addNamedOutput(job, "", TextOutputFormat.class, Text.class, Text.class);



    }
}
