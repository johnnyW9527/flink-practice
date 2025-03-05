package com.johnny.flink.basic.window;

import com.johnny.flink.model.CarInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.runtime.operators.window.assigners.CountTumblingWindowAssigner;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/26 14:22
 */
public class CountWindowDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> sourceDs = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<CarInfo> mapDs = sourceDs.map(new MapFunction<String, CarInfo>() {
            @Override
            public CarInfo map(String s) throws Exception {
                String[] split = s.split(",");
                return new CarInfo(split[0], Integer.parseInt(split[1]));
            }
        });

        SingleOutputStreamOperator<CarInfo> numsDs = mapDs.keyBy(CarInfo::getSensorId)
                .countWindow(5L).sum("nums");

        SingleOutputStreamOperator<CarInfo> numsDs2 = mapDs.keyBy(CarInfo::getSensorId)
                .countWindow(5L, 3L)
                .sum("nums");
        env.execute();


    }

}
