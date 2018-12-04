package com.hmdu.bigdata.hiveUDF;

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 10:11 2018/7/7
 * @ 根据输入的字符串和key，找出valud
 * 示例
 * 输入：sex=1&hight=180&weight=130&sal=28000  key='weight'
 * 输出：130
 */
public class JsonParser extends UDF {
    public static String evaluate(String json, String key) {
        if (Strings.isEmpty(json) || Strings.isEmpty(key)) {
            return null;
        }
        // 1.将传入的字符串改成json的形式
        String jonStr = "{" + json.replace("&", ",")
                .replace("=", ":") + "}";

        try {
            // 2.创建一个json的对象，用来对json形式的字符串进行操作
            JSONObject jo = new JSONObject(jonStr);
            return jo.getString(key);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        System.out.println(new JsonParser().evaluate(
                "sex=1&hight=180&weight=130&sal=28000", "sal"));
    }
}
