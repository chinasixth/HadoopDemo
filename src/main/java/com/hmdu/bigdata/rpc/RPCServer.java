package com.hmdu.bigdata.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:12 2018/6/26
 * @ rpc服务器端：
 * 首先构造一个服务，根据配置对象将这个服务构建出来以后，要给服务指定这个服务具体是什么类型服务，
 * 并设置版本控制类，设置监听的url和监听的地址，最后根据设置的信息将服务真正的建立成我们想要的那个服务类型
 * 然后启动服务，这样就会一直对指定的地址的端口进行监控
 */
public class RPCServer implements Hello {

    @Override
    public String say(String words) {
        System.out.println(words);

        return "received datanode01 heartbeat......";
    }

    public static void main(String[] args) {
        try {
            // 构造一个服务
            RPC.Server server = new RPC.Builder(new Configuration())
                    .setInstance(new RPCServer())
                    .setProtocol(Hello.class)
                    .setBindAddress("127.0.0.1")
                    .setPort(6666)
                    .build();

            // 将服务启动起来
            server.start();

            System.out.println("service started...");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
