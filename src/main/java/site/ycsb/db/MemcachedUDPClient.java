package site.ycsb.db;


import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;
import org.apache.log4j.Logger;

import java.text.MessageFormat;


/**
 * Concrete Memcached udp client implementation.
 */
public class MemcachedUDPClient implements Runnable  {

    private com.whalin.MemCached.MemCachedClient client;
    public SockIOPool pool;
    private final Logger logger = Logger.getLogger(getClass());



    public MemcachedUDPClient() {}


    public MemcachedUDPClient(boolean isTcp) throws Exception {

        // String[] serverlist = {"127.0.0.1:11212"};         
        // SockIOPool pool = SockIOPool.getInstance("test",false);
        // //pool.setBufferSize(1024*1024*16);
        // pool.setServers(serverlist);      
        // pool.initialize();  

        // try {
        //     this.client =createMemcachedClient();
        // } catch (Exception e) {
        //     throw new RuntimeException(e);
        // }

        // try {
        //     String[] serverlist = {"192.168.238.142:11211"};
        //     SockIOPool pool = SockIOPool.getInstance("test",isTcp);
        //     //pool.setBufferSize(1024*1024*16);
        //     pool.setServers(serverlist);
        //     pool.initialize();
        //     client = createMemcachedClient(isTcp);
        // } catch (Exception e) {
        //     throw new RuntimeException(e);
        // }
    }

    public void run() {
        
    }

    public void init(boolean isTcp,String poolName) throws Exception {
        try {
            String[] serverlist = {"169.254.27.101:11211"}; // 有线
            //String[] serverlist = {"192.168.0.102:11211"}; // 无线
            //String[] serverlist = {"192.168.238.142:11212"}; // 虚拟机
            //String[] serverlist = {"192.168.0.103:11211"}; // windows虚拟机 无线
            //String[] serverlist = {"169.254.242.161:11211"}; // windows虚拟机 有线
            //String[] serverlist = {"172.23.238.213:11211"}; // wsl
            //String[] serverlist = {"169.254.27.111:11211"}; // ubuntu上的ubuntu
            this.pool = SockIOPool.getInstance(poolName,isTcp);
            //pool.setBufferSize(1024*1024*16);
            this.pool.setServers(serverlist);
            this.pool.initialize();
            client = createMemcachedClient(isTcp,poolName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected com.whalin.MemCached.MemCachedClient memcachedClient() {
        return this.client;
    }

    protected com.whalin.MemCached.MemCachedClient createMemcachedClient(boolean isTcp,String poolName)
            throws Exception {
        // 第一个参数表示使用udp协议，第二个参数表示使用文本协议
        return new com.whalin.MemCached.MemCachedClient(poolName, isTcp, false);
    }

    protected static String createQualifiedKey(String table, String key) {
        return MessageFormat.format("{0}-{1}", table, key);
    }

}