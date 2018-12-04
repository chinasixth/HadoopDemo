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

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 9:30 2018/7/2
 * @
 */
public class MultiOutputFiles {

    public static Text k = new Text();

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String first = line.substring(0, 1);
            k.set(first);
            context.write(k, value);
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private static final Text v = new Text("");

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;

            for (Text value : values) {
                context.write(value, v);
                count ++;
            }

            k.set(String.valueOf(count));
            context.write(k, v);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();

        try {
            Job job = Job.getInstance(conf, "MultiOutputFiles");
            job.setJarByClass(MultiOutputFiles.class);

            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 设置partitioner的相关属性
            job.setPartitionerClass(MyPartitioner.class);
            // 设置reduce的个数，该值最好是跟分区数量相等
            job.setNumReduceTasks(Integer.parseInt(args[2]));

           job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            boolean isok = job.waitForCompletion(true);

            System.exit(isok ? 0 : 1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
