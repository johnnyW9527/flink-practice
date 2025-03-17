package com.johnny.flink.basic.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;
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
 * @date 2025/2/28 17:01
 */
public class StateDemo2 {

    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(3000);
        env.setStateBackend(new FsStateBackend("file:///D:/clp"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));

        DataStreamSource<String> resultDs = env.addSource(new MyKafkaSource());
        resultDs.print();
        env.execute();
    }



    public static class MyKafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction
    {
        // 1. 声明一个OperatorState来记录
        private ListState<Long> offsetState = null;
        private Long offset = 0L;
        private boolean flag = true;


        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            offsetState.clear();
            offsetState.add(offset);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("offsetState", Long.class);
            offsetState = context.getOperatorStateStore().getListState(descriptor);
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            Iterator<Long> iterator = offsetState.get().iterator();
            if (iterator.hasNext()) {
                offset = iterator.next();
            }
            while (flag) {
                offset += 1;
                int id = getRuntimeContext().getIndexOfThisSubtask();
                sourceContext.collect("分区:" + id + "消费到,offset:" + offset);
                TimeUnit.SECONDS.sleep(2);
                if (offset % 5 == 0) {
                    throw new Exception("程序遇到异常了.....");
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
