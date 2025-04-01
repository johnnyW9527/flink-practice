package com.johnny.flink.webproject.job;

import com.johnny.flink.webproject.model.OrderEvent;
import com.johnny.flink.webproject.model.OrderResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.time.Duration;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/4/1 10:53
 */
public class OrderTimeoutWithoutCep {

    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        URL resource = OrderPayTimeout.class.getResource("/temp/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> inputDs = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L));

        SingleOutputStreamOperator<OrderResult> resultDs = inputDs.keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());
        resultDs.print("payed normally");
        resultDs.getSideOutput(orderTimeoutTag).print("payed timeout");


        env.execute();
    }

    public static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {

        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreatedState;
        ValueState<Long> timeTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<>("is-payed-state", Boolean.class));
            isCreatedState = getRuntimeContext().getState(new ValueStateDescriptor<>("is-created-state", Boolean.class));
            timeTs = getRuntimeContext().getState(new ValueStateDescriptor<>("time-ts-state", Long.class));
        }

        @Override
        public void processElement(OrderEvent orderEvent, KeyedProcessFunction<Long, OrderEvent, OrderResult>.Context context, Collector<OrderResult> collector) throws Exception {
            boolean isPayed = isPayedState.value() != null && isPayedState.value();
            boolean isCreated = isCreatedState.value() != null && isCreatedState.value();
            long value = timeTs.value() == null ? 0L : timeTs.value();

            if ("create".equals(orderEvent.getEventType())) {
                if (isPayed) {
                    collector.collect(new OrderResult(orderEvent.getOrderId(), "payed successed"));
                    isCreatedState.clear();
                    isPayedState.clear();
                    timeTs.clear();
                } else {
                    long ts = orderEvent.getTimestamp() * 1000L + 5 * 60 * 1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    timeTs.update(ts);
                    isCreatedState.update(true);
                }
            } else if ("pay".equals(orderEvent.getEventType())) {
                if (isCreated) {
                    if (orderEvent.getTimestamp() * 1000L < value) {
                        collector.collect(new OrderResult(orderEvent.getOrderId(), "payed successed"));
                    }
                } else {
                    context.output(orderTimeoutTag, new OrderResult(orderEvent.getOrderId(), "payed timeout"));
                }
                isCreatedState.clear();
                isPayedState.clear();
                timeTs.clear();
                context.timerService().deleteEventTimeTimer(value);
            } else {
                // 如果没有下单事件，乱序，注册一个定时器，等待下单事件
                context.timerService().registerEventTimeTimer(orderEvent.getTimestamp() * 1000L );
                // 更新状态
                timeTs.update(orderEvent.getTimestamp() * 1000L);
                isPayedState.update(true);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, OrderEvent, OrderResult>.OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            if (isPayedState.value() != null && isPayedState.value()) {
                // 如果有pay 说明create没来
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed but not found created log"));
            } else {
                // 如果没pay 支付超时
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed timeout"));
            }
            isPayedState.clear();
            isCreatedState.clear();
            timeTs.clear();
        }
    }

}
