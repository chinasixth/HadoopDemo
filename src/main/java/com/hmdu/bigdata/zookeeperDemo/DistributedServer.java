package com.hmdu.bigdata.zookeeperDemo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 20:56 2018/6/26
 */
public class DistributedServer {

    private static final String CONNECT_STRING = "hadoop05:2181,hadoop07:2181,hadoop08:2181";
    private static final int SESSION_TIMEOUT = 2000;

    private static final CountDownLatch latch = new CountDownLatch(1);

    private static ZooKeeper zk = null;

    private static final String PARENT_NODE = "/servers";


    /*
     * 获取zookeeper的连接客户端
     */
    public static void getConnection() throws IOException, InterruptedException {
        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected &&
                    latch.getCount() > 0) {
                latch.countDown();
            }
        });
        latch.await();
    }

    /*
     * 向zookeeper集群注册服务器信息
     * */
    public static void registerServer(String hostname) throws KeeperException, InterruptedException {
        // 判断要注册的服务器是否存在(服务器就是节点)
        Stat stat = zk.exists(PARENT_NODE, null);

        // 如果父节点不存在，则线创建父节点
        if (stat == null) {
            zk.create(PARENT_NODE, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 创建服务器节点，这一步不需要判断节点是否存在
        String path = zk.create(PARENT_NODE + "/server", hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname + " is online..." + path);
    }

    /*
     * 业务类
     * */
    public static void handleBussiness(String hostname) throws InterruptedException {
        System.out.println(hostname + "start working...");
        Thread.sleep(Long.MAX_VALUE);
    }


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        // 创建一个分布式服务器类
        DistributedServer ds = new DistributedServer();

        // 获取客户端连接
        ds.getConnection();

        // 往zk上注册自己
        ds.registerServer(args[0]);

        // 启动业务逻辑
        ds.handleBussiness(args[0]);
    }
}
