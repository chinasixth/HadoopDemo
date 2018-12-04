package com.hmdu.bigdata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:07 2018/7/2
 * @ 定义类的方式
 * 1. public class Model01 implements Tool
 * 2. public class Model01 extends ToolRunner implements Tool
 * 3. public class Model01 extends Configured implements Tool
 *
 * 实现的功能
 * run
 * getConf
 * setConf
 */
public class Model01 implements Tool {

    private static final Logger LOGGER = Logger.getLogger(Model01.class);

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "");
        job.setJarByClass(Model01.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean isok = job.waitForCompletion(true);

        return isok ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
    }

    @Override
    public Configuration getConf() {
        return new Configuration();
    }

    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new Configuration(), new Model01(), args));
        } catch (Exception e) {
            LOGGER.error("主函数运行异常", e);
        }
    }
}
