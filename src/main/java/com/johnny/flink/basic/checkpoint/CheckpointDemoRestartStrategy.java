package com.johnny.flink.basic.checkpoint;

import org.apache.commons.lang.SystemUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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
 * @date 2025/3/8 20:45
 */
public class CheckpointDemoRestartStrategy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint参数设置
        // 设置checkpoint的时间间隔为1000ms做一次checkpoint（其实就是每隔1000ms发一次barrier）
        env.enableCheckpointing(1000);
        if(SystemUtils.IS_OS_WINDOWS){
            env.setStateBackend(new FsStateBackend("file:///D:/ckp"));
        }else{
            env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink-checkpoint/checkpoint"));
        }

        // 设置两个checkpoint之间最少等待时间，如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是  false不是
        //默认值为0，表示不容忍任何检查点失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);

        //-4.失败率重启--偶尔使用
        //5分钟内重启3次(第3次不包括,也就是最多重启2次),每次间隔10s
        /*env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, // 每个测量时间间隔最大失败次数
                Time.of(5, TimeUnit.MINUTES), //失败率测量的时间间隔
                Time.of(10, TimeUnit.SECONDS) // 每次重启的时间间隔
        ));*/

        //上面的能看懂就行,开发中使用下面的代码即可
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        //2.Source
        DataStream<String> linesDS = env.socketTextStream("node1", 9999);

        //3.Transformation
        //3.1切割出每个单词并直接记为1
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = linesDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //value就是每一行
                String[] words = value.split(" ");
                for (String word : words) {
                    if(word.equals("bug")){
                        System.out.println("手动模拟的bug...");
                        throw new RuntimeException("手动模拟的bug...");
                    }
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        //3.2分组
        //注意:批处理的分组是groupBy,流处理的分组是keyBy
        KeyedStream<Tuple2<String, Integer>, String> groupedDS = wordAndOneDS.keyBy(t -> t.f0);
        //3.3聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = groupedDS.sum(1);

        //4.sink
        result.print();
        env.execute();
    }

}
