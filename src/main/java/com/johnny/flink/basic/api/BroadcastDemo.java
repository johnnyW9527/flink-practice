package com.johnny.flink.basic.api;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 20:28
 */
public class BroadcastDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<> (
            "broadcast-state", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
        );

        // 学生数据集（学号、姓名）
        BroadcastStream<Tuple2<Integer, String>> broadcastStream = env.fromCollection(Arrays.asList(Tuple2.of(1, "张三"), Tuple2.of(2, "李四"), Tuple2.of(3, "王五")))
                .broadcast(descriptor);

        // 成绩数据集（学号、学科、成绩）
        DataStreamSource<Tuple3<Integer, String, Integer>> scoreDs =
                env.fromCollection(Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英文", 86)));

        SingleOutputStreamOperator<String> resultDs = scoreDs.connect(broadcastStream)
                .process(new BroadcastProcessFunction<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>, String>() {
                    @Override
                    public void processElement(Tuple3<Integer, String, Integer> integerStringIntegerTuple3, BroadcastProcessFunction<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                        String name = readOnlyContext.getBroadcastState(descriptor).get(integerStringIntegerTuple3.f0+"");
                        collector.collect(name + "的" + integerStringIntegerTuple3.f1 + "成绩为：" + integerStringIntegerTuple3.f2);
                    }

                    @Override
                    public void processBroadcastElement(Tuple2<Integer, String> integerStringTuple2, BroadcastProcessFunction<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>, String>.Context context, Collector<String> collector) throws Exception {
                        context.getBroadcastState(descriptor).put(integerStringTuple2.f0+"", integerStringTuple2.f1);
                        System.out.println("广播状态更新：" + integerStringTuple2.f0 + "->" + integerStringTuple2.f1);
                    }
                });
        resultDs.print();

        env.execute();


    }

}
