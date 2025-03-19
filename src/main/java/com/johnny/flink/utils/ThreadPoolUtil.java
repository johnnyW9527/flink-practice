package com.johnny.flink.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
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
 * @date 2025/3/19 10:59
 */
public class ThreadPoolUtil {

    private static Logger logger = LoggerFactory.getLogger(ThreadPoolUtil.class);

    /**
     * 线程池对象
     */
    private ThreadPoolExecutor executor;

    /**
     * 空参构造器 4个最少线程 10个最大线程 等待60s
     */
    public ThreadPoolUtil() {
        initThreadPoolExecutor(4, 10, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    }

    public ThreadPoolUtil(int corePoolSize, int maximumPoolSize, long keepAliveTime) {
        initThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    }

    public ThreadPoolUtil(int corePoolSize, int maximumPoolSize) {
        initThreadPoolExecutor(corePoolSize, maximumPoolSize, 180, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    }

    /**
     * 初始化线程池执行器
     *
     * @param corePoolSize 核心线程池大小，即线程池中始终存在的线程数
     * @param maximumPoolSize 最大线程池大小，即线程池中允许的最大线程数
     * @param keepAliveTime 线程空闲时间，当线程池中的线程数量超过核心线程数时，多余的空闲线程在终止前等待新任务的最长时间
     * @param timeUnit 时间单位，定义了keepAliveTime参数的时间单位
     * @param workQueue 工作队列，用于保存等待执行的任务
     *
     * 此方法用于配置和创建一个线程池，根据给定的参数，线程池能够以合适的方式处理并发任务
     */
    private void initThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit timeUnit, BlockingQueue<Runnable> workQueue) {
        executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, timeUnit, workQueue);
        logger.info(
                "##### 创建线程池成功，其中 线程池维护线程的最少数量 = {}，线程池维护线程的最大数量 = {}， 线程池维护线程所允许的空闲时间(秒) = {} ",
                corePoolSize,
                maximumPoolSize,
                keepAliveTime
        );
    }

    public ThreadPoolExecutor getThreadPoolExecutor() {
        return executor;
    }

}
