package com.johnny.flink.basic.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 11:00
 */
public class TransformationDemo2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> ds1 = env.fromElements("hadoop", "spark", "flink");
        DataStreamSource<String> ds2 = env.fromElements("hadoop", "spark", "flink");
        DataStreamSource<Long> ds3 = env.fromElements(1L, 2L, 3L);

        // 合并不去重
        DataStream<String> result = ds1.union(ds2);
        ConnectedStreams<String, Long> tempResult = ds1.connect(ds3);

        SingleOutputStreamOperator<String> result2 = tempResult.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String s) throws Exception {
                return "String -> String:" + s;
            }

            @Override
            public String map2(Long aLong) throws Exception {
                return "Long -> String:" + aLong.toString();
            }
        });
        result.print();
        result2.print();

        env.execute();

    }

}
