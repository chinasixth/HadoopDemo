package com.hmdu.bigdata.mr.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:52 2018/7/2
 * @ 找两个用户的共同好友，第一步
 */
public class CommonFriends {

    public static class CommonFriendMapper extends Mapper<LongWritable, Text, Text, Text>{
        private static Text k = new Text();
        private static Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split(":");
            String person = splits[0];
            String[] friends = splits[1].split(",");

            v.set(person);

            for (String friend : friends) {
                k.set(friend);

                context.write(k, v);
            }
        }
    }

    public static class CommomFriendReducer extends Reducer<Text,Text,Text,Text>{
        private static Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            Iterator<Text> it = values.iterator();
            while (it.hasNext()) {
                Text next = it.next();
                sb.append(next.toString() + ",");
            }

            String users = sb.toString();
            users.substring(0, users.length()-1);
            v.set(sb.toString());
            context.write(key, v);
        }
    }

}
