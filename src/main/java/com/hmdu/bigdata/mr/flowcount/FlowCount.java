package com.hmdu.bigdata.mr.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:46 2018/7/2
 * @
 */
public class FlowCount  {
    public static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        private static Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] flows = line.split("\t");

            String phoneNum = flows[0];
            Long upFlow = Long.parseLong(flows[flows.length - 3]);
            Long downFlow = Long.parseLong(flows[flows.length - 2]);

            FlowBean flowBean = new FlowBean(phoneNum, upFlow, downFlow);

            k.set(phoneNum);
            context.write(k, flowBean);

        }
    }

    public static class FlowReducer extends Reducer<Text, FlowBean, FlowBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            Iterator<FlowBean> iterator = values.iterator();
            Long upFlow;
            Long downFlow;

            while (iterator.hasNext()) {
                FlowBean flowBean = iterator.next();

                upFlow = flowBean.getUpFlow();
                downFlow = flowBean.getDownFlow();

            }
        }
    }
}
