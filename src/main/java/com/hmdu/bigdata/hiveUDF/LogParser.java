package com.hmdu.bigdata.hiveUDF;

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static jodd.datetime.JDateTimeDefault.locale;


/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:21 2018/7/7
 * @ 解析url中的一些信息
 * 示例：
 * 输入：220.181.108.151 - - [31/Jan/2012:00:02:32 +0800] \"GET /home.php?mod=space&uid=158&do=album&view=me&from=space HTTP/1.1\" 200 8784 \"-\" \"Mozilla/5.0
 * 输出：220.181.108.151	20120131 120232	GET	/home.php?mod=space&uid=158&do=album&view=me&from=space	HTTP	200	Mozilla
 */
public class LogParser extends UDF {
    private static String evaluate(String url) {
        if (Strings.isEmpty(url)) {
            return null;
        }
        // 定义一个正则表达式
        // 括号里面是需要提取的，不需要提取的就直接匹配过去
        String regex = "^([0-9.]+\\d+) - - \\[(.* \\+\\d+)\\] .+(GET|POST) (.+) (HTTP)\\S+ (\\d+) .+\\\"(\\w+).+$";

        // 加载规则
        Pattern pattern = Pattern.compile(regex);

        // 构造匹配器
        Matcher matcher = pattern.matcher(url);

        // 定义一个变长字符串用来拼接结果
        StringBuilder sb = new StringBuilder();

        // 判断是否匹配成功
        if (matcher.find()) {
            // 获取匹配上的总数量
            int count = matcher.groupCount();

            // 循环匹配结果,这里要注意到matcher.group(i)的i，取值是从0开始的
            for (int i = 0; i < count; i++) {
                // 对第二个匹配结果进行一些操作
                // 因为第二个匹配到的时间格式，跟我们想要的不一样，所以要对格式进行修改
                if (i == 1) {
                    // 定义需要转换成的日期格式
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd hhmmss");

                    // 解读日期格式
                    try {
                        // 将解读出来的日期格式转换成date
                        Date date = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", locale.ENGLISH).parse(matcher.group(i+1));

                        // 将date转换成自己想要的格式
                        String dataFormat = sdf.format(date);

                        // 拼接字符串
                        sb.append(dataFormat + "\t");
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                } else {
                    // 拼接字符串
                    sb.append(matcher.group(i+1) + "\t");
                }
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String s = "220.181.108.151 - - [31/Jan/2012:00:02:32 +0800] \\\"GET /home.php?mod=space&uid=158&do=album&view=me&from=space HTTP/1.1\\\" 200 8784 \\\"-\\\" \\\"Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)\\\"";
        System.out.println(new LogParser().evaluate(s));
    }
}
