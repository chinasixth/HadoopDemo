package com.hmdu.bigdata.mapreduceapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 框架在调用自己写的ma业务方法时，会将数据作为参数(一个key，一个value)
 * 传递给map方法。
 * 在默认情况下，框架传入的key就是从待处理的文件中读取到的某一行的偏移量，所以可以用Long
 * 框架传入的value就是从待处理的文件中读取到的某一行的内容，可以用String
 * <p>
 * 但是，Long和String是java的原生类，它的序列化的效率比较低，所以hadoop对其进行了改造，
 * Long   对应  LongWritable
 * String 对应  Text
 * <p>
 * 自定义的wordcount(词频统计)
 * <p>
 * input
 * <p>
 * map阶段(输入数据的没每一行执行一个map方法)
 * KEYIN：行偏移量，每一行的第一个字母距离该文件的首位置的距离
 * VALUEIN：行的值，每一行的内容
 * KEYOUT：map阶段输出的key的值
 * VALUEOUT：map阶段输出的value的值
 * 注意：map阶段输出的key/value的类型要和reduce阶段输入的key/value的值相同
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * <p>
 * reduce阶段(map阶段输出的数据，相同key的执行一次reduce方法)
 * reduce阶段的输出key/value是根据需求，自己选择的类型
 * Reducer<Text, IntWritable, Text, IntWritable>
 */
public class MyWordCount {
    // 自定义mapper类，重写父类中的mapper方法
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // 定义map阶段输出值的类型对象，在这里定义，每一个map方法就不用再去定义了，减少内存消耗
        public static Text k = new Text();
        public static IntWritable v = new IntWritable();

        // ctrl + o 重写父类方法快捷键
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 1.从输入数据中获取每个文件中每一行的值(每一行调用一个map方法)
            // 将maptask传递给map方法的数据内容进行类型转换
            String line = value.toString();

            // 2.对每一行的数据进行切分(有的不用)
            String[] words = line.split(" ");

            // 3.循环处理(这里输出)
            // itet快速生成foreach
            for (String word : words) {
                k.set(word);
                v.set(1);
                // map阶段的输出
                context.write(k, v);
            }
        }
    }

    // map阶段输出的数据在输入到reduce阶段之前，会进行一次操作
    //    // 这个操作会将所有的key相同的value,合并到一个集合中，也就是Iterable<IntWritable>迭代器
    //    // 例如：
    //    // map阶段输出数据如下：
    //    // hadoop 1
    //    // Hadoop 1
    //    // 那么reduce阶段输入的数据就是
    //    // hadoop {1, 1}

    // 自定义reducer类，重写父类中的reducer方法
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // redece方法的调用规律：框架会从map端的输出结果里挑出所有key-value的数据对组成一组
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // 自定义一个计数器
            int counter = 0;

            for (IntWritable i : values) {
                counter += i.get();
            }
            // reduce的输出
            context.write(key, new IntWritable(counter));
        }
    }

    // 驱动
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取配置信息的对象
        Configuration conf = new Configuration();

        // 2.对conf进行设置(没有就不用设置)

        // 3.获取job对象，并为job起一个名字
        Job job = Job.getInstance(conf, "mywordcount");

        // 4.设置job的运行主类
        job.setJarByClass(MyWordCount.class);

        // 5.对map阶段进行设置
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);

        // 这里是设置输入数据的路径，以及将输入数据输入到哪一个Job，args[0]命令行输入的第一个参数
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 6.对reduce阶段设置
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置把哪个Job输出的数据放到哪个目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7.提交Job(作业)，参数为true，表示要打印信息，
        // job.submit(); 这个提交并不对过程进行监控
        int isok = job.waitForCompletion(true) ? 0 : 1;

        // 8.退出job
        System.exit(isok);

    }
}


