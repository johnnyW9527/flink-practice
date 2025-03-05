package com.johnny.flink.basic.window;

import com.johnny.flink.model.Order;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
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
 * @date 2025/2/28 15:55
 */
public class WatermakerDemo2 {

    public static void main(String[] args) throws Exception {
        FastDateFormat df = FastDateFormat.getInstance("HH:mm:ss");
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

        SingleOutputStreamOperator<Order> waterDs = sourceDs.assignTimestampsAndWatermarks(new WatermarkStrategy<Order>() {
            @Override
            public WatermarkGenerator<Order> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Order>() {

                    private int userId = 0;
                    private long eventTime = 0L;
                    private final long outOfOrdernessMillis = 3000L;
                    private long maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;


                    @Override
                    public void onEvent(Order order, long l, WatermarkOutput watermarkOutput) {

                        userId = order.getUId();
                        eventTime = order.getCurrTime();
                        maxTimestamp = Math.max(maxTimestamp, eventTime);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                        //Watermaker = 当前最大事件时间 - 最大允许的延迟时间或乱序时间
                        Watermark watermark = new Watermark(maxTimestamp - outOfOrdernessMillis - 1);
                        System.out.println("key:" + userId + ",系统时间:" + df.format(System.currentTimeMillis()) + ",事件时间:" + df.format(eventTime) + ",水印时间:" + df.format(watermark.getTimestamp()));
                        watermarkOutput.emitWatermark(watermark);
                    }
                };
            }
        }.withTimestampAssigner((element, recordTimestamp) -> element.getCurrTime()));

        SingleOutputStreamOperator<String> result = waterDs.keyBy(Order::getOrderId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Order, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Order> input, Collector<String> out) throws Exception {
                        //准备一个集合用来存放属于该窗口的数据的事件时间
                        List<String> eventTimeList = new ArrayList<>();
                        for (Order order : input) {
                            Long eventTime = order.getCurrTime();
                            eventTimeList.add(df.format(eventTime));
                        }
                        String outStr = String.format("key:%s,窗口开始结束:[%s~%s),属于该窗口的事件时间:%s",
                                key, df.format(window.getStart()), df.format(window.getEnd()), eventTimeList);
                        out.collect(outStr);
                    }
                });

        result.print();

        env.execute();

    }

}
