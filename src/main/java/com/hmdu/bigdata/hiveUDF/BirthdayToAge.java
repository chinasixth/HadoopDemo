package com.hmdu.bigdata.hiveUDF;

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Calendar;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 8:51 2018/7/7
 * @ 根据输入的日期，计算输出年龄
 * 示例；
 * 输入：1979-07-06
 * 输入：39
 */
public class BirthdayToAge extends UDF {
    public static int evaluate(String birth) {
        // 1.判断输入字符串是否为空，如果为空，返回-1
        if (Strings.isEmpty(birth)) {
            return -1;
        }

        // 2.将输入日期截取出来年月日
        String[] split = birth.split("-");

        int year = Integer.parseInt(split[0]);
        int month = Integer.parseInt(split[1]);
        int day = Integer.parseInt(split[2]);

        // 3.获取今天的年月日
        Calendar calendar = Calendar.getInstance();
        int nowYear = calendar.get(Calendar.YEAR);
        // 这里要注意，月份是从0开始的
        int nowMonth = calendar.get(Calendar.MONTH) + 1;
        int nowDay = calendar.get(Calendar.DAY_OF_MONTH);

        // 4.比较
        int age = nowYear - year;

        if (nowMonth < month) {
            age -= 1;
        } else if (month == nowMonth && nowDay < day) {
            age -= 1;
        }
        return age;
    }

    // 为了测试方便，写一个main方法
    public static void main(String[] args) {
        System.out.println(new BirthdayToAge().evaluate("2000-01-01"));
    }
}
