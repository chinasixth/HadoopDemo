package com.hmdu.bigdata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:19 2018/6/28
 * 计算学生的平均成绩
 */
public class AvgGrade {
    private static Text k = new Text();
    private static DoubleWritable v = new DoubleWritable();


    public class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Double avg = 0.0;
        private Double total = 0.0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1.
            String line = value.toString();

            // 2.
            String[] words = line.split(" ");
            String studentName = words[0];

            // 3.
            total = 0.0;
            for (int i = 1; i < words.length; i++) {
                String word = words[i];
                total += Double.valueOf(word);
            }

            // 4.
            avg = total / 3;

            k.set(studentName);
            v.set(avg);

            // 5.
            context.write(k, v);


        }
    }

    /*
    * 如果map端将任务做完，可以不用写reduce端
    * 但是map端一定要有*/
//    public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
//        @Override
//        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
//        }
//    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取配置对象
        Configuration conf = new Configuration();

        // 2.创建一个Job对象
        Job job = Job.getInstance(conf, "high temperature");

        // 3.指定job的jar执行路径
        job.setJarByClass(AvgGrade.class);

        // 4.指定本job的map业务类以及map方法的输出类型
        job.setMapperClass(MyMapper.class);

        // 可以直接使用setOUtputKeyClass();
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // 5.指定本job的reduce业务类以及reduce方法的输出类型
//        job.setReducerClass(MyReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(DoubleWritable.class);

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
