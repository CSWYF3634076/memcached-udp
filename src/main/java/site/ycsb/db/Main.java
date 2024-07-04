package site.ycsb.db;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import java.util.Random;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

import org.apache.commons.math3.distribution.ZipfDistribution;



public class Main {
    Random random = new Random();
    public static void multThread() throws Exception{
        final int N = 1;
        final int OP_CNT = 100;
        Thread[] threads = new Thread[N];
        int k;
        final int modnum = 0x7b;
        for(k = 0 ; k < N ; k++){
            threads[k] = new Thread(new Runnable() { 
                @Override
                public void run() {
                    try {
                        MemcachedUDPClient muc = new MemcachedUDPClient(false);
                        muc.init(false,"pool"+Thread.currentThread().getId());
                        MemCachedClient memCachedClient = muc.memcachedClient();
                        ZipfDistribution zipfDistribution = new ZipfDistribution(OP_CNT, 0.001);

                        long startTimeThread = System.currentTimeMillis();
                        for (int i = 0; i < OP_CNT; i++) {
                            //String[] keys = {String.format("key%d100000000", i++),String.format("key%d100000000", i++),String.format("key%d100000000", i)};
                            //Object value = memCachedClient.getMulti(keys);
                            //Object value = memCachedClient.get("key" + i%100+100000000); 
                            Object value = memCachedClient.get("key" + (int)(zipfDistribution.sample()%modnum) +100000000);
                            if(value == null){
                                System.out.printf("%s get failed\n",Thread.currentThread().getName());
                            }else{
                                System.out.printf("%s get success %s\n",Thread.currentThread().getName(),value);
                            }
                        }

                        long endTimeThread = System.currentTimeMillis();
                        float latency = (float)(endTimeThread - startTimeThread)*1000/OP_CNT;
                        float throughput = (float)(OP_CNT*N)/(endTimeThread - startTimeThread);
                        System.out.printf("%s finished %s time cost: %dms  Latency: %fμs  Throughput: %fKReq/s\n",Thread.currentThread().getName(),
                        muc.pool.getSock("pool"+Thread.currentThread().getId()).toString(),(endTimeThread - startTimeThread),latency,throughput);
                        
                    }
                    catch (Exception ex) {System.out.println(ex.toString());}
                }  
            });
        }

        long startTime = System.currentTimeMillis();
        for(k = 0 ; k < N ; k++){
            threads[k].start();
        }
        for(k = 0 ; k < N ; k++){
            threads[k].join();
        }
        long endTime = System.currentTimeMillis();
        //System.out.println(k+"线程 程序运行时间：" + (endTime - startTime) + "ms" + " 吞吐量："+(10000*N)/);
        System.out.printf("%d线程 程序运行时间：%dms   吞吐量：%fKReq/s",k,endTime - startTime,(float)(OP_CNT*N)/(endTime - startTime));
    }
    

    public static void association() throws Exception{
        final int N = 1;
        final int OP_CNT = 100;
        Thread[] threads = new Thread[N];
        int k;
        for(k = 0 ; k < N ; k++){
            threads[k] = new Thread(new Runnable() { 
                @Override
                public void run() {
                    try {
                        MemcachedUDPClient muc = new MemcachedUDPClient(false);
                        muc.init(false,"pool"+Thread.currentThread().getId());
                        MemCachedClient memCachedClient = muc.memcachedClient();
                        ZipfDistribution zipfDistribution = new ZipfDistribution(OP_CNT, 0.99);

                        long startTimeThread = System.currentTimeMillis();
                        for (int i = 0; i < OP_CNT; i++) {
                            //String[] keys = {String.format("key%d100000000", i++),String.format("key%d100000000", i++),String.format("key%d100000000", i)};
                            //Object value = memCachedClient.getMulti(keys);
                            Object value = memCachedClient.get("key" + i+100000000); 
                            //Object value = memCachedClient.get("key" + zipfDistribution.sample() +100000000);
                            if(value == null){
                                System.out.printf("%s get failed\n",Thread.currentThread().getName());
                            }else{
                                System.out.printf("%s get success %s\n",Thread.currentThread().getName(),value);
                            }
                        }

                        long endTimeThread = System.currentTimeMillis();
                        float latency = (float)(endTimeThread - startTimeThread)*1000/OP_CNT;
                        float throughput = (float)(OP_CNT*N)/(endTimeThread - startTimeThread);
                        System.out.printf("%s finished %s time cost: %dms  Latency: %fμs  Throughput: %fKReq/s\n",Thread.currentThread().getName(),
                        muc.pool.getSock("pool"+Thread.currentThread().getId()).toString(),(endTimeThread - startTimeThread),latency,throughput);
                        
                    }
                    catch (Exception ex) {System.out.println(ex.toString());}
                }  
            });
        }

        long startTime = System.currentTimeMillis();
        for(k = 0 ; k < N ; k++){
            threads[k].start();
        }
        for(k = 0 ; k < N ; k++){
            threads[k].join();
        }
        long endTime = System.currentTimeMillis();
        //System.out.println(k+"线程 程序运行时间：" + (endTime - startTime) + "ms" + " 吞吐量："+(10000*N)/);
        System.out.printf("%d线程 程序运行时间：%dms   吞吐量：%fKReq/s",k,endTime - startTime,(float)(OP_CNT*N)/(endTime - startTime));
    }
    

    public static void coldBoot() throws Exception{
        final int N = 1;
        final int OP_CNT = 100;
        Thread[] threads = new Thread[N];
        int k;
        for(k = 0 ; k < N ; k++){
            threads[k] = new Thread(new Runnable() { 
                @Override
                public void run() {
                    try {
                        MemcachedUDPClient muc = new MemcachedUDPClient(true);
                        muc.init(true,"pool"+Thread.currentThread().getId());
                         
                        MemCachedClient memCachedClient = muc.memcachedClient();
                        long startTimeThread = System.currentTimeMillis();
                        for (int i = 0; i < OP_CNT; i++) {
                            //String[] keys = {String.format("key%d100000000", i++),String.format("key%d100000000", i++),String.format("key%d100000000", i)};
                            //Object value = memCachedClient.getMulti(keys);
                            //Object value = memCachedClient.get("key" + i+100000000);
                            long key_start = (Thread.currentThread().getId()%N)*100; // N*100 = 用户态写好的感知的数量
                            String key = String.format("key%d100000000", key_start + i);
                            Object value = memCachedClient.get(key);
                            
                            if(value == null){
                                System.out.printf("%s get failed\n",Thread.currentThread().getName());
                            }else{
                                System.out.printf("%s get success %s\n",Thread.currentThread().getName(),value);
                            }

                            
                        }
                        long endTimeThread = System.currentTimeMillis();
                        float latency = (float)(endTimeThread - startTimeThread)*1000/OP_CNT;
                        float throughput = (float)(OP_CNT*N)/(endTimeThread - startTimeThread);
                        System.out.printf("%s finished %s time cost: %dms  Latency: %fμs  Throughput: %fKReq/s\n",Thread.currentThread().getName(),
                        muc.pool.getSock("pool"+Thread.currentThread().getId()).toString(),(endTimeThread - startTimeThread),latency,throughput);
                        
                    }
                    catch (Exception ex) {System.out.println(ex.toString());}
                }
            });
        }

        long startTime = System.currentTimeMillis();
        for(k = 0 ; k < N ; k++){
            threads[k].start();
        }
        for(k = 0 ; k < N ; k++){
            threads[k].join();
        }
        long endTime = System.currentTimeMillis();
        //System.out.println(k+"线程 程序运行时间：" + (endTime - startTime) + "ms" + " 吞吐量："+(10000*N)/);
        System.out.printf("%d线程 程序运行时间：%dms   吞吐量：%fKReq/s",k,endTime - startTime,(float)(OP_CNT*N)/(endTime - startTime));
    }
    
    public static void test() throws Exception{
        MemcachedUDPClient muc = new MemcachedUDPClient(true);
        muc.init(true,"pool"+new Random(1000).nextInt());
        MemCachedClient memCachedClient = muc.memcachedClient();
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < 101; i++) {
            //i += 15;
            Object value = memCachedClient.get("key" + i+100000000);
            //String[] keys = {"key0100000000","key1100000000","key1100000000"};
            //String[] keys = {"key0100000000"};
            //Object value = memCachedClient.getMulti(keys);
            if(value == null){
                System.out.println("get failed");
            }else{ 
                System.out.println("get success "+value);
            }
            //Thread.sleep(50);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");

    }

    public static void addDate() throws Exception{
        MemcachedUDPClient muc = new MemcachedUDPClient(false);
        muc.init(false,"pool"+new Random(1000).nextInt());
        MemCachedClient memCachedClient = muc.memcachedClient();
        for (int i = 0; i < 10000; i++) {
            boolean ok = memCachedClient.add("key" + i+100000000, "value" + i+100000000);
            if(!ok){
                System.out.println("add failed");
            }else{
                System.out.println("add success");
            }
        }
    }
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        //addDate();
        //test();
        multThread();
        association();
        



//        for (int j = 0; j < 10; j++) {
//            for (int i = 0; i < 100; i++) {
//                int sample = zipfDistribution.sample();
//                //System.out.print("sample[" + sample + "]: ");
//                System.out.print(sample+" ");
//            }
//            System.out.println();
//        }



        return;    


    }
}