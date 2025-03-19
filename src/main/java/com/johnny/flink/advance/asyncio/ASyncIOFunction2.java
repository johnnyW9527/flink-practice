package com.johnny.flink.advance.asyncio;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/18 11:03
 */
public class ASyncIOFunction2 extends RichAsyncFunction<CategoryInfo, CategoryInfo> {

    private transient MysqlSyncClient mysqlSyncClient;
    // 线程池
    private ExecutorService executor;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mysqlSyncClient = new MysqlSyncClient();
        executor = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    }

    @Override
    public void asyncInvoke(CategoryInfo categoryInfo, ResultFuture<CategoryInfo> resultFuture) throws Exception {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                resultFuture.complete(Collections.singletonList(mysqlSyncClient.query(categoryInfo)));
            }
        });
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void timeout(CategoryInfo input, ResultFuture<CategoryInfo> resultFuture) throws Exception {
        System.out.println("async call time out!");
        input.setName("未知");
        resultFuture.complete(Collections.singleton(input));
    }
}
