package com.johnny.flink.basic.window;

import com.johnny.flink.model.CarInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
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
 * @date 2025/2/26 14:28
 */
public class SessionWindowDemo1 {

    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.Source
        DataStreamSource<String> socketDS = env.socketTextStream("node1", 9999);

        SingleOutputStreamOperator<CarInfo> cartInfoDS = socketDS.map(new MapFunction<String, CarInfo>() {
            @Override
            public CarInfo map(String value) throws Exception {
                String[] arr = value.split(",");
                return new CarInfo(arr[0], Integer.parseInt(arr[1]));
            }
        });

        SingleOutputStreamOperator<CarInfo> result = cartInfoDS.keyBy(CarInfo::getSensorId)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .sum("nums");

        result.print();

        env.execute();
    }

}
