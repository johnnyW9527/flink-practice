package com.johnny.flink.basic.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/28 16:55
 */
public class StateDemo1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Long>> tupleDS = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 6L),
                Tuple2.of("上海", 8L),
                Tuple2.of("北京", 3L),
                Tuple2.of("上海", 4L)
        );

        SingleOutputStreamOperator<Tuple2<String, Long>> result1 = tupleDS.keyBy(data -> data.f0)
                .maxBy(1);

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> resultDs = tupleDS.keyBy(data -> data.f0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>() {

                    private transient ValueState<Long> maxValueState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("maxValueState", Long.class);
                        maxValueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Tuple3<String, Long, Long> map(Tuple2<String, Long> value) throws Exception {

                        Long historyMaxValue = maxValueState.value();
                        Long currentValue = value.f1;
                        if (historyMaxValue == null || currentValue > historyMaxValue) {
                            maxValueState.update(currentValue);
                            return Tuple3.of(value.f0, currentValue, currentValue);
                        } else {
                            return Tuple3.of(value.f0, currentValue, historyMaxValue);
                        }

                    }
                });

        resultDs.print();

        env.execute();
    }


}
