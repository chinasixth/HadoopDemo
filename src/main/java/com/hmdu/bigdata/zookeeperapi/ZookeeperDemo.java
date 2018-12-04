package com.hmdu.bigdata.zookeeperapi;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 15:05 2018/6/25
 */
public class ZookeeperDemo {

    private static ZooKeeper zk = null;
    // 进程锁的使用，只有当count=0时，才会解锁，让进程继续执行下面的操作
    // 设置1就是上锁
    private static CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 建立一个zookeeper的客户端连接
        // 第一个参数要连接的地址端口
        // 第二个参数超时
        // 第三个参数监听
        // 在连接的时候创建了一个监听器，也就是默认监听器
        zk = new ZooKeeper("hadoop05:2181,hadoop07:2181,hadoop08:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 连接的状态
                // 判断等待的时间是否执行完成，这里等待的事件就是zk连接成功
                // 当zk连接成功，调用countDown方法进行解锁
                if (latch.getCount() > 0 && watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    latch.countDown();
                }
                System.out.println(watchedEvent.getPath());
                System.out.println(watchedEvent.getState());
                System.out.println(watchedEvent.getType());

                // 可以实现循环监听
                List<String> zkList = null;
                try {
                    // 注册监听器，也就是获取一个节点，并注册监听器
                    zkList = zk.getChildren("/", true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (String child : zkList) {
                    System.out.println(child);
                }
            }
        });

        // 等待事件完成
        latch.await();
        // 第二个参数为true，表示要使用默认的监听器，也就是使用自己创建zk的时候创建的监听器
        // 第一次获取列表，并注册监听
        List<String> zkList = zk.getChildren("/", true);
        for (String child : zkList) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

}
