package com.hmdu.bigdata.hiveUDF;

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 15:45 2018/7/7
 * @ 将json字符串，解析成一个类
 */
public class JsonUDF extends UDF {
    public static String evaluate(String json) {
        if (Strings.isEmpty(json)) {
            return null;
        }

        // json字符串解析器，可以将json字符串直接解析成一个类，传入的参数是要解析成的类的.class文件，
        // 要注意，这个.class文件中的属性，要和json字符串中的字段名相同
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonRateBean bean = objectMapper.readValue(json, JsonRateBean.class);
            return bean.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        System.out.println(new JsonUDF().evaluate("{\"movie\":\"1193\",\"rate\":\"5\",\"timeStamp\":\"978300760\",\"uid\":\"1\"}\n"));
    }
}
