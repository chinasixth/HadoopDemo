package com.hmdu.bigdata.mr.filereader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 16:13 2018/7/2
 * @
 */
public class WholeFileReader extends RecordReader<NullWritable, BytesWritable> {
    // 文件切片信息，
    private FileSplit fileSplit;

    // 配置文件
    private Configuration conf;
    // 返回值，记录本次读取的内容
    private BytesWritable value = new BytesWritable();

    // 判断什么时候结束,false表示未结束
    private boolean processed = false;

    // 初始化类
    // 记录外部传入的参数
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        // 初始化，配置文件和切片
        this.fileSplit = (FileSplit) inputSplit;
        this.conf = taskAttemptContext.getConfiguration();
    }

    // 读数据
    @Override
    public boolean nextKeyValue() throws IOException {
        // 判断读取文件时候结束
        if (!processed) {
            // 定义一个变量，存储读取内容
            byte[] content = new byte[(int) fileSplit.getLength()];

            // 获取文件的路径
            Path file = fileSplit.getPath();

            // 获取文件操作对象，看看文件是哪个文件系统
            FileSystem fs = file.getFileSystem(conf);

            // 打开文件，获取文件输入流
            FSDataInputStream in = fs.open(file);

            // 整体读取文件内容
            IOUtils.readFully(in, content, 0, content.length);

            // 将读取的内容设置为全局变量，以供getCurrentValue获取当前的内容
            value.set(content, 0, content.length);


            processed = true;
            IOUtils.closeStream(in);
        }
        return true;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() {
        // do nothing
    }
}
