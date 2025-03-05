package com.johnny.flink.basic.window;

import com.johnny.flink.model.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/28 15:49
 */
public class WatermakerDemo1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Order> sourceDs = env.addSource(new SourceFunction<Order>() {

            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> sourceContext) throws Exception {
                Random rand = new Random();
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int uId = rand.nextInt(3);
                    long orderPrice = rand.nextInt(101);
                    long eventTime = System.currentTimeMillis() - rand.nextInt(5) * 1000;
                    sourceContext.collect(new Order(orderId, uId, orderPrice, eventTime));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        SingleOutputStreamOperator<Order> watermakerDs = sourceDs.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, recordTimestamp) -> element.getCurrTime()));

        SingleOutputStreamOperator<Order> result = watermakerDs.keyBy(Order::getOrderId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("orderPrice");
        result.print();

        env.execute();

    }

}
