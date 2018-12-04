package com.hmdu.bigdata.hbasemapreduce;

import com.hmdu.bigdata.hbaseapicopy.hbaseutils.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:11 2018/7/13
 * @ 数据存放在hdfs上，需要读取数据，然后处理并存放到hbase中
 */
public class HdfsToHbase {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 转换
            String line = value.toString();

            // 分割字符串
            String[] split = line.split("\t");

            // 获取年龄
            String age = split[0].split(":")[1];

            k.set(age);
            context.write(k, new IntWritable(1));
        }
    }

    public static class MyReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        private ImmutableBytesWritable k = new ImmutableBytesWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //
            Iterator<IntWritable> iterator = values.iterator();
            int count = 0;

            while (iterator.hasNext()) {
                IntWritable next = iterator.next();
                count += next.get();
            }

            k.set(Bytes.toBytes(key.toString()));

            // table写数据，需要传入的数据是所有更新hbase数据的对象，put、delete
            // 也就是对数据进行操作的一些对象
            // Put在new的时候指定的参数就是rowkey的值，rowkey这个列是不用指定列名的，默认的列名就是ROW
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("f1"),
                    Bytes.toBytes("result"),
                    Bytes.toBytes(count + ""));

            context.write(k, put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取configuration对象
        Configuration conf = new Configuration();

        // 设置连接参数
        conf.set("hbase.zookeeper.quorum", "hadoop05:2181,hadoop07:2181,hadoop08:2181");
        conf.set("fs.defaultFS", "hdfs://liuhao");
        // 高可用还需要配置更多的属性，所以直接弄配置文件

        // 获取job对象
        Job job = Job.getInstance(conf, "hdfstohbase");

        job.setJarByClass(HdfsToHbase.class);

        // 设置map端的参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        createTable("ns2:pcResult");

        // 设置reduce端的参数
        // 指定表名的时候可以不用指定namespace
        TableMapReduceUtil.initTableReducerJob("ns2:pcResult", MyReducer.class, job);

        // 设置数据的来源，
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }

    private static void createTable(String tableName){
        TableName table = TableName.valueOf(tableName);
        if (Strings.isEmpty(tableName)) {
            return ;
        }

        // 操作表是ddl操作，先获得admin对象
        Admin admin = HbaseUtils.getAdmin();

        // 先判断表是否存在,如果存在，把表删掉，重新创建
        try {
            if (admin.tableExists(table)) {
                if (admin.isTableEnabled(table)){
                    admin.disableTable(table);
                }

                admin.deleteTable(table);
            }

            HTableDescriptor ht = new HTableDescriptor(table);

            HColumnDescriptor hc = new HColumnDescriptor(Bytes.toBytes("f1"));
            hc.setBloomFilterType(BloomType.ROWCOL);
            hc.setVersions(1, 3);

            ht.addFamily(hc);

            admin.createTable(ht);

            HbaseUtils.closeAdmin(admin);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
