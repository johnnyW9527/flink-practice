package com.johnny.flink.basic.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
 * @date 2025/2/25 10:54
 */
public class TransformationDemo1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> lineDs = env.socketTextStream("node01", 9999);

        DataStream<String> wordsDs = lineDs.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] values = s.split(" ");
                for (String value : values) {
                    collector.collect(value);
                }
            }
        });

        SingleOutputStreamOperator<String> filterDs = wordsDs.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !s.equals("heihei");
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDs = filterDs.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> groupDs = mapDs.keyBy(t -> t.f0);

        DataStream<Tuple2<String, Integer>> result1 = groupDs.sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result2 = groupDs.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                return Tuple2.of(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
            }
        });
        result1.print("result1");
        result2.print("result2");

        env.execute();
    }

}
