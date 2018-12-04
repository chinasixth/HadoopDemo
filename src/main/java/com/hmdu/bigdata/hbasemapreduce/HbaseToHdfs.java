package com.hmdu.bigdata.hbasemapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:05 2018/7/13
 * @ 需求：将hbase表中的数据，以列名和值结合的方式组织起来，然后存储到hdfs上
 */
public class HbaseToHdfs {
    // 数据源是hbase，所以继承的是TableMapper
    public static class MyMapper extends TableMapper<Text, NullWritable> {
        private Text k = new Text();

        @Override
            protected void map(ImmutableBytesWritable key,
                           Result value,
                           Context context)
                throws IOException, InterruptedException {

            // 解析get到的数据
            // hbase中的数据是一行一行的往map方法中传输，一次传输就是一个get的结果集
            // 处理结果集，将数据拼接成hdfs需要的格式
            CellScanner cellScanner = value.cellScanner();

            StringBuffer sb = new StringBuffer();

            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();

                sb.append(new String(CellUtil.cloneQualifier(cell), "utf-8")
                        + ":" + new String(CellUtil.cloneValue(cell), "utf-8")
                        + "\t");
            }

            k.set(sb.toString().getBytes());
            context.write(k, NullWritable.get());
        }
    }
    /*
    * map将数据输入以后，就已经达到了我们的要求，所以不用写reduce端了
    * */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取configuration对象
        Configuration conf = new Configuration();

        // 设置连接参数
        conf.set("hbase.zookeeper.quorum", "hadoop05:2181,hadoop07:2181,hadoop08:2181");
        conf.set("fs.defaultFS", "hdfs://liuhao");
        // 高可用还需要配置更多的属性，所以直接弄配置文件

        // 获取job对象
        Job job = Job.getInstance(conf, "hbasetohdfs");

        // 设置程序的执行路径
        job.setJarByClass(HbaseToHdfs.class);

        // 设置map端的输出参数
        // 因为没有reduce，所以可以直接设置out
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置数据的来源
        TableMapReduceUtil.initTableMapperJob("ns2:t1",
                new Scan(), MyMapper.class, Text.class,
                NullWritable.class, job);

        // 设置输出文件路径
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }

    // 这里设置的scan是要对rowkey进行筛选
//    private static Scan getScan(){
//        Scan scan = new Scan();
//        scan = scan.setStartRow(Bytes.toBytes("00001" + "\000"));
//        return scan;
//    }
}
