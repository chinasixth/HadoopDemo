package com.hmdu.bigdata.mr.filereader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 16:41 2018/7/2
 * @
 */
public class SmallFilesCombiner extends ToolRunner implements Tool {

    public static class MyMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private static Text filePath = new Text();
        @Override
        protected void setup(Context context) {
            InputSplit split = context.getInputSplit();
            FileSplit split1 = (FileSplit) split;
            Path path = split1.getPath();
            filePath = new Text(path.toString());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

            context.write(filePath, value);
        }
    }

    @Override
    public int run(String[] args) {
        return 0;
    }

    @Override
    public void setConf(Configuration conf) {
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
