package com.johnny.flink.advance.asyncio;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * <b>使用异步的方式读取redis的数据</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/18 11:15
 */
public class AsyncRedis extends RichAsyncFunction<String, String> {


    //定义redis的连接池对象
    private JedisPoolConfig config = null;

    private static String ADDR = "localhost";
    private static int PORT = 6379;
    //等待可用连接的最大时间，单位是毫秒，默认是-1，表示永不超时，如果超过等待时间，则会抛出异常
    private static int TIMEOUT = 10000;
    //定义redis的连接池实例
    private JedisPool jedisPool = null;
    //定义连接池的核心对象
    private Jedis jedis = null;
    //初始化redis的连接
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //定义连接池对象属性配置
        config = new JedisPoolConfig();
        //初始化连接池对象
        jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT);
        //实例化连接对象（获取一个可用的连接）
        jedis = jedisPool.getResource();
    }



    @Override
    public void close() throws Exception {
        super.close();
        if(jedis.isConnected()){
            jedis.close();
        }
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        System.out.println("input:" + input);
        // 发起一个异步请求，返回结果
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                String[] arrayData = input.split(",");
                String name = arrayData[1];
                return jedis.hget("AsyncReadRedis", name);
            }
        }).thenAccept((String dbResult) -> {
            resultFuture.complete(Collections.singletonList(dbResult));
        });
    }

    @Override
    public void timeout(String input, org.apache.flink.streaming.api.functions.async.ResultFuture<String> resultFuture) throws Exception {
        System.out.println("redis connect timeout!");
    }
}
