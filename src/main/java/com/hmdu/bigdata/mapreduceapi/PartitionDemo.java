package com.hmdu.bigdata.mapreduceapi;/**
 * @ Author     ：liuhao
 * @ Date       ：Created in 19:26 2018/6/24
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 分区：
 * 输入数据
 * Hello HI HI qianfeng
 * hi hi qianfeng qianfeng
 * 163.com
 * qq.com
 * 189.com
 * @163.com
 * @qq.com
 *
 */
public class PartitionDemo {

    public static Text k = new Text();
    public static FloatWritable v = new FloatWritable();

    public class MyMapper extends Mapper<Text, LongWritable, Text, Text> {

        // 仅在map阶段执行之前执行一次，通常用于初始化操作
        @Override
        protected void setup(Context context){

        }

        @Override
        protected void map(Text key, LongWritable value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            for (String s : words) {
                context.write(new Text(s), new Text(1+""));
            }
        }

        // 仅在整个map方法执行完以后执行一次
        @Override
        protected void cleanup(Context context){

        }
    }

    public class MyReducer extends Reducer<Text, Text, Text, Text> {
        // 在reduce阶段执行之前执行一次


        @Override
        protected void setup(Context context) {
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int counter = 0;
            for (Text text : values) {
                counter += Integer.parseInt(text.toString());
            }
            context.write(key, new Text(counter+""));
        }

        // 在reduce阶段执行完以后执行一次
        @Override
        protected void cleanup(Context context){
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "partitionJob");
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 设置分区
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(4);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int result = job.waitForCompletion(true) ? 0 : 1;
        System.exit(result);
    }

}































