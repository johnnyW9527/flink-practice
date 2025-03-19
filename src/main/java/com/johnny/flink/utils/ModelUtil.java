package com.johnny.flink.utils;

import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TernaryBoolean;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import java.util.Collection;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

/**
 * <b>模块工具类</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/18 19:39
 */
public class ModelUtil {

    public static ResourceBundle localConfig = ResourceBundle.getBundle("localConfig");
    public static ResourceBundle config = ResourceBundle.getBundle("config");

    public static Logger log = LoggerFactory.getLogger(ModelUtil.class);

    /**
     * 根据键获取配置值
     * 该方法根据操作系统类型选择不同的配置源
     * 在Windows操作系统中，它会从本地配置中获取值；
     * 而在其他操作系统中，则从可能的远程或外部配置源中获取值
     *
     * @param key 配置项的键
     * @return 对应键的配置值
     */
    public static String getConfigValue(String key) {
        // 判断操作系统是否为Windows
        if (SystemUtils.IS_OS_WINDOWS) {
            // 如果是Windows系统，从本地配置中获取配置值
            return localConfig.getString(key);
        }
        // 如果不是Windows系统，从其他配置源中获取配置值
        return config.getString(key);
    }


    /**
     * 部署文件系统(FS)检查点配置
     *
     * @param env StreamExecutionEnvironment环境，用于配置检查点
     * @param applicationName 应用程序名称，用于区分不同的应用检查点路径
     * @param interval 检查点的时间间隔，单位为毫秒
     */
    public static void deployFsCheckpoint(StreamExecutionEnvironment env, String applicationName, long interval) {
        // 启动checkpoint,设置为精确一次，并通过传入的参数设置时间间隔
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(interval);

        //设置状态后端
        String checkpointPath = null;
        // 根据操作系统类型选择合适的检查点路径
        if (SystemUtils.IS_OS_WINDOWS) {
            checkpointPath = ModelUtil.getConfigValue("fs.checkpoint.path") + applicationName + "\\\\";
        } else {
            // 对于非Windows系统，构建OBS路径
            String obsPath = "obs://" +
                    ModelUtil.getConfigValue("obs.ak") + ":" +
                    ModelUtil.getConfigValue("obs.sk") + "@" +
                    ModelUtil.getConfigValue("obs.endpoint") +
                    ModelUtil.getConfigValue("fs.checkpoint.path");
            checkpointPath = obsPath + applicationName + "/";
        }
        env.setStateBackend(new FsStateBackend(checkpointPath));

        // 设置可容忍的检查点失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);

        // 启用外部化检查点，并设置检查点在取消时保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置检查点的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(interval * 10);

        // 设置最大并发检查点数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);

        // 记录环境设置日志信息
        log.info(">>>>> 正在进行环境设置，会创建fs的checkpoint环境，applicationName：" + applicationName + " ; 间隔时间interval：" + interval + " ; ");
    }

    public static void deployRocksdbCheckpoint(StreamExecutionEnvironment env, String applicationName, long interval) {

        // 启动checkpoint，设置为精确一次，并通过传入的参数设置时间间隔
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(interval);

        // 设置 状态后端 和 checkpoint 为 filesystem 的模式，并通过配置文件指定文件夹（判断是本地调试还是集群环境）
        String checkpointPath = null;
        if (SystemUtils.IS_OS_WINDOWS) {
            checkpointPath = ModelUtil.getConfigValue("rocksdb.checkpoint.path") + applicationName + "\\\\";
        } else {
            String obsPath = "obs://" +
                    ModelUtil.getConfigValue("obs.ak") + ":" +
                    ModelUtil.getConfigValue("obs.sk") + "@" +
                    ModelUtil.getConfigValue("obs.endpoint") +
                    ModelUtil.getConfigValue("rocksdb.checkpoint.path");
            checkpointPath = obsPath + applicationName + "/";
        }
        RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(new FsStateBackend(checkpointPath), TernaryBoolean.TRUE);
        // 预定义选项，SPINNING_DISK_OPTIMIZED为基于磁盘的优化，一般使用SPINING_DISK_OPTIMIZED_HIGH_MEM，但这会消耗比较多的内存
        rocksDbBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        rocksDbBackend.setRocksDBOptions(new RocksDBOptionsFactory() {
            @Override
            public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                return currentOptions
                        // 指定信息日志文件的最大大小。 如果当前日志文件大于' max_log_file_size '，一个新的信息日志文件将被创建。如果为0，所有日志将被写入一个日志文件。
                        .setMaxLogFileSize(64 * 1024 * 1024)
                        // 信息日志文件的最大保留个数。
                        // .setKeepLogFileNum(3)
                        ;
            }

            @Override
            public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                return currentOptions;
            }
        });
        env.setStateBackend(rocksDbBackend);

        // 设置2个checkpoint之间的最小间隔，不需要设置，默认为0
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(interval);

        // 设置能容忍100个检查点的失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);

        // 当作业被cancel时，不删除外部保存的检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 当在设置的时间内还没有保存成功认为该检查点失败，设置为interval的10倍
        env.getCheckpointConfig().setCheckpointTimeout(interval * 10);

        // 设置同时可以进行10个checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);

        log.info(">>>>> 正在进行环境设置，会创建rocksdb的checkpoint环境，applicationName：" + applicationName + " ; 间隔时间interval：" + interval + " ; ");
    }

    public static void deployRestartStrategy(StreamExecutionEnvironment env) {

        // 当任务中异常失败后，会重启任务3次，间隔时间为60秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(60, TimeUnit.SECONDS)));
        log.info(">>>>> 正在进行环境设置，重启策略为：重启任务3次，间隔时间为60秒");

        // 10分钟内重启5次,每次间隔2分钟（排除了网络等问题，如果再失败，需要手动查明原因）
        // env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(10, TimeUnit.MINUTES), Time.of(2, TimeUnit.MINUTES)));
        // log.info(">>>>> 正在进行环境设置，重启策略为：10分钟内重启5次,每次间隔2分钟");

    }


}
