package com.hmdu.bigdata.mr.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 15:10 2018/7/2
 * @
 */
public class CommonFriendsStep2 {
    public static class CommobFriendsStep02Mapper extends Mapper<LongWritable,Text,Text,Text>{
        private static Text k = new Text();
        private static Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(" ");
            String friend = split[0];
            String[] users = split[1].split(",");
            Arrays.sort(users);
            v.set(friend);

            for (int i = 0; i < users.length - 1; i++) {
                for (int j = i + 1; j < users.length; j++) {
                    String s = users[i] + users[j];
                    k.set(s);
                    context.write(k, v);
                }
            }

        }
    }

    public static class CommonFriendsStep02Reducer extends Reducer<Text, Text, Text, Text>{
        private static Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            StringBuffer sb = new StringBuffer();
            while (iterator.hasNext()) {
                Text next =  iterator.next();
                sb.append(next.toString() + ",");
            }

            String friends = sb.toString();
            friends = friends.substring(0, friends.length() - 1);
            v.set(friends);
            context.write(key, v);
        }
    }

}
