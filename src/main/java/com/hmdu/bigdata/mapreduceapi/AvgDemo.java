package com.hmdu.bigdata.mapreduceapi;/**
 * @ Author     ：liuhao
 * @ Date       ：Created in 16:57 2018/6/24
 * @ Modified By：
 * @Version: 1.1.0$
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
 * 将汽车生产流水线的三个时间段的记录算一个平均值
 * line     zao      wu      ye
 * L_1      393      430     276
 * L_2      388      560     333
 * L_3      450      600     312
 *
 * 要求输出
 * 生产线          生产平均值
 * L_1              *
 * L_2              *
 * L_3              *
 */
public class AvgDemo {
    public static Text k = new Text();
    public static FloatWritable v = new FloatWritable();

    public class MyMapper extends Mapper<Text, LongWritable, Text, FloatWritable>{

        // 仅在map阶段执行之前执行一次，通常用于初始化操作
        @Override
        protected void setup(Context context){

        }

        @Override
        protected void map(Text key, LongWritable value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            String lineName = words[0];
            int zao = Integer.parseInt(words[1]);
            int wu = Integer.parseInt(words[2]);
            int ye = Integer.parseInt(words[3]);
            k.set(lineName);
            v.set((float) ((zao+wu+ye)*1.0/(words.length-1)));
            context.write(k, v);
        }

        // 仅在整个map方法执行完以后执行一次
        @Override
        protected void cleanup(Context context){

        }
    }

    public class MyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
        // 在reduce阶段执行之前执行一次


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.write(new Text("生产线\t生产平均值"), new FloatWritable());
        }

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, new FloatWritable(values.iterator().next().get()));
        }

        // 在reduce阶段执行完以后执行一次
        @Override
        protected void cleanup(Context context){
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "avgJob");
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int result = job.waitForCompletion(true) ? 0 : 1;
        System.exit(result);

    }

}
