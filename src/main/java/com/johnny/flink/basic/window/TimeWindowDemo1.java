package com.johnny.flink.basic.window;

import com.johnny.flink.model.CarInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/26 11:29
 */
public class TimeWindowDemo1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> sourceDs = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<CarInfo> mapDs = sourceDs.map(new MapFunction<String, CarInfo>() {
            @Override
            public CarInfo map(String s) throws Exception {
                String[] words = s.split(",");
                return new CarInfo(words[0], Integer.parseInt(words[1]));
            }
        });

        SingleOutputStreamOperator<CarInfo> numsDs = mapDs.keyBy(CarInfo::getSensorId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum("nums");

        SingleOutputStreamOperator<CarInfo> nums = mapDs.keyBy(CarInfo::getSensorId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum("nums");


        env.execute();
    }

}
