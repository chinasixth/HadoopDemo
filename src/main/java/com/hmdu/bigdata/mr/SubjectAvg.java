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
 * @ Date   ：Created in 14:52 2018/6/28
 */
public class SubjectAvg {
    private static Text k = new Text();
    private static DoubleWritable v = new DoubleWritable();


    public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");

            k.set("语文");
            v.set(Double.parseDouble(words[1]));
            context.write(k, v);

            k.set("数学");
            v.set(Double.parseDouble(words[2]));
            context.write(k, v);

            k.set("英语");
            v.set(Double.parseDouble(words[3]));
            context.write(k, v);
        }
    }

    /*
     * 如果map端将任务做完，可以不用写reduce端
     * 但是map端一定要有
     * */

    public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double total = 0;
            for (DoubleWritable score : values) {
                total += score.get();
                count++;
            }

            v.set(total / count);
            context.write(key, v);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取配置对象
        Configuration conf = new Configuration();

        // 2.创建一个Job对象
        Job job = Job.getInstance(conf, "Subject avg");

        // 3.指定job的jar执行路径
        job.setJarByClass(SubjectAvg.class);

        // 4.指定本job的map业务类以及map方法的输出类型
        job.setMapperClass(MyMapper.class);

        // 可以直接使用setOUtputKeyClass();
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

        // 指定reduceTask的数目，一个reduceTask对应一个输出part
        job.setNumReduceTasks(Integer.valueOf(args[2]));

        // 8.提交job
        boolean isok = job.waitForCompletion(true);

        // 9.退出
        System.exit(isok ? 0 : 1);
    }
}
