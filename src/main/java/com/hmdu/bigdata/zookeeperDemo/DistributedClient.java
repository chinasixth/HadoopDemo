package com.hmdu.bigdata.zookeeperDemo;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 21:35 2018/6/26
 */
public class DistributedClient {
    /*
     * 客户端是获得服务器列表，并注册一个监听，
     * 当服务器列表发生变化时，客户端能够重新获得服务器列表
     * */
    private static final String CONNECT_STRING = "hadoop05:2181,hadoop07:2181,hadoop08:2181";
    private static final int SESSION_TIMEOUT = 2000;

    private static final CountDownLatch latch = new CountDownLatch(1);

    private static ZooKeeper zk = null;

    private static final String PARENT_NODE = "/servers";
    // volatile修饰的成员变量在每次被线程访问时，都强迫从共享内存中重读该成员变量的值。
    // 而且，当成员变量发生变化时，强迫线程将变化值回写到共享内存。
    // 这样在任何时刻，两个不同的线程总是看到某个成员变量的同一个值。
    /*
     * 线程可见，类里面的一个成员变量，可能有多个线程都要使用，有的线程是修改数据，有的可能是读取数据
     * 如果一个线程在这修改数据的同时，其他线程要读数据，读的数据可能是之前的老数据
     * 要让数据立即可见，这个线程修改，其他线程能立即看到改变后的数据
     * 加volatile就是所有的线程都读一份数据
     * */
    private volatile static List<String> serverLists = null;


    /*
     * 获取zookeeper的连接客户端
     */
    public static void getConnection() throws IOException, InterruptedException {
        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (latch.getCount() > 0 && watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    latch.countDown();
                }

                try {
                    getServerList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        latch.await();
    }

    public static void getServerList() throws KeeperException, InterruptedException {
        // 获取父节点中的子节点信息，并对父节点注册一个监听
        List<String> servers = zk.getChildren(PARENT_NODE, true);

        // 创建一个list，用来存储子节点的名称(子节点的数据)，也就是服务器名
        ArrayList<String> serverList = new ArrayList<>();

        // 循环子节点信息，获取节点的数据(服务器名称)，并保存到临时变量里
        for (String server : servers) {
            byte[] data = zk.getData(PARENT_NODE + "/" + server, false, null);
            serverList.add(new String(data));
        }

        serverLists = serverList;

        // 打印服务器列表
        System.out.println(serverList);
    }

    /*
     * 业务功能*/
    public static void handleBussiness() throws InterruptedException {
        System.out.println("client start working...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        // 获取client对象
        DistributedClient client = new DistributedClient();

        // 获取zk对象
        client.getConnection();

        // 获取服务器信息列表
        client.getServerList();

        // 启动业务逻辑
        client.handleBussiness();
    }


}
