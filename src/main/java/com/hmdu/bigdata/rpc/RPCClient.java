package com.hmdu.bigdata.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:13 2018/6/26
 * @ rpc客户端：
 * 首先使用RPC的getPRroxy方法获取RPC客户端的代理类，也就是客户端类实现的接口，用来向服务器发送消息
 */
public class RPCClient implements Hello {

    @Override
    public String say(String words) {
        return "success";
    }

    public static void main(String[] args) {
        try {
            while (true) {
                Hello hello = RPC.getProxy(Hello.class, 1L,
                        new InetSocketAddress("127.0.0.1", 6666),
                        new Configuration());
                String ret = hello.say("I am datanode01, I am live...");
                System.out.println(ret);

                Thread.sleep(3000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
