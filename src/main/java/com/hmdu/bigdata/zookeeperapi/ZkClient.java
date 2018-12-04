package com.hmdu.bigdata.zookeeperapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 16:30 2018/6/25
 */
public class ZkClient {
    private static ZooKeeper zk = null;
    // 进程锁的使用
    private static CountDownLatch latch = new CountDownLatch(1);


    @Before
    public void init() throws IOException, InterruptedException {
        zk = new ZooKeeper("hadoop05:2181,hadoop07:2181,hadoop08:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 连接的状态
                if (latch.getCount() > 0 && watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    latch.countDown();
                }
                System.out.println(watchedEvent.getPath());
                System.out.println(watchedEvent.getState());
                System.out.println(watchedEvent.getType());

            }
        });
        latch.await();
    }

    /**
     * 创建znode节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void create() throws KeeperException, InterruptedException {
        String s = zk.create("/zkdata", "hello zk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(s);
    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void testExist() throws KeeperException, InterruptedException {
        Stat stat = zk.exists("/zkdta", false);
        System.out.println(stat == null ? "not exists" : "exists");
    }

    /**
     * 获取节点的所有子节点信息
     */
    @Test
    public void getChildRen() throws KeeperException, InterruptedException {
        List<String> list = zk.getChildren("/", true);
        for (String child : list) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 获取节点数据
     */
    @Test
    public void getData() throws KeeperException, InterruptedException {
        byte[] zkData = zk.getData("/zkdata", true, null);
        System.out.println(new String(zkData, 0, zkData.length));
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 删除节点
     */
    @Test
    public void deleteNode() throws KeeperException, InterruptedException {
        zk.delete("/zkdata", -1);
    }

    /**
     * 设置节点数据
     */
    @Test
    public void setData() throws KeeperException, InterruptedException {
        zk.setData("/zkdta", "hello memeda".getBytes(), -1);
    }
}
