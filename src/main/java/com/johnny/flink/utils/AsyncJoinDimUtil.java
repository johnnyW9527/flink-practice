package com.johnny.flink.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/19 10:55
 */
public abstract class AsyncJoinDimUtil<T> extends RichAsyncFunction<T, T> implements AsyncJoinFunction<T> {

    private static Logger logger = LoggerFactory.getLogger(AsyncJoinDimUtil.class);

    /**
     * 线程池
     */
    protected ThreadPoolExecutor executor;

    protected String tableName;

    protected AsyncJoinDimUtil() {
    }

    protected AsyncJoinDimUtil(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ThreadPoolUtil threadPoolUtil = new ThreadPoolUtil();
        executor = threadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    // 通过维度表获取维度信息
                    JSONObject dimInfo = getDimInfo(tableName, t);
                    if (dimInfo != null) {
                        join(t, dimInfo);
                    }
                    resultFuture.complete(Collections.singletonList(t));
                } catch (Exception e) {
                    logger.error("@@@@@ 关联维表失败（关联时抛出异常），传入的数据为：{}，抛出的异常为：{}", t, e.getMessage());
                }
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        resultFuture.complete(Collections.singletonList(input));
        logger.error("@@@@@ 关联维表超时（获取维度数据超时），已将传入数据直接传出（没有关联维度），传入的数据为：{}", input);
    }
}
