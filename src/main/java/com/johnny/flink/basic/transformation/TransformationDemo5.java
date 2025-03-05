package com.johnny.flink.basic.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 11:16
 */
public class TransformationDemo5 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.Source
        DataStream<String> linesDS = env.readTextFile("data/input/words.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = linesDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //3.Transformation
        // 全部发往第一个task
        DataStream<Tuple2<String, Integer>> result1 = tupleDS.global();
        // 广播
        DataStream<Tuple2<String, Integer>> result2 = tupleDS.broadcast();
        // 上下游并发度一样时一对一发送
        DataStream<Tuple2<String, Integer>> result3 = tupleDS.forward();
        // 随机均匀分配
        DataStream<Tuple2<String, Integer>> result4 = tupleDS.shuffle();
        // Round-Robin轮流分配
        DataStream<Tuple2<String, Integer>> result5 = tupleDS.rebalance();
        // Local Round-Robin 本地轮流分配
        DataStream<Tuple2<String, Integer>> result6 = tupleDS.rescale();
        // 自定义单播
        DataStream<Tuple2<String, Integer>> result7 = tupleDS.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                return key.equals("hello") ? 0 : 1;
            }
        }, t -> t.f0);

        //4.sink
        //result1.print();
        //result2.print();
        //result3.print();
        //result4.print();
        //result5.print();
        //result6.print();
        result7.print();

        //5.execute
        env.execute();
    }
}
