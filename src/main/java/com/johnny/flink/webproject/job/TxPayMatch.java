package com.johnny.flink.webproject.job;

import com.johnny.flink.webproject.model.OrderEvent;
import com.johnny.flink.webproject.model.ReceipEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.time.Duration;

/**
 * <b>支付账单核对</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/4/1 13:44
 */
public class TxPayMatch {

    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays"){};

    private final static OutputTag<ReceipEvent> unmatchedReceips = new OutputTag<ReceipEvent>("unmatched-receips"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        URL resource = TxPayMatch.class.getResource("/temp/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> inputDs = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L))
                .filter(data -> !"".equals(data.getTxId()));

        URL resource1 = TxPayMatch.class.getResource("/temp/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceipEvent> receipDs = env.readTextFile(resource1.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceipEvent(fields[0], fields[1], new Long(fields[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ReceipEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L));

        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceipEvent>> resultDs = inputDs.keyBy(OrderEvent::getTxId)
                .connect(receipDs.keyBy(ReceipEvent::getTxId))
                .process(new TxPayMatchDetect());
        resultDs.print("matched-pays");
        resultDs.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultDs.getSideOutput(unmatchedReceips).print("unmatched-receips");
        env.execute();
    }

    public static class TxPayMatchDetect extends CoProcessFunction<OrderEvent, ReceipEvent, Tuple2<OrderEvent, ReceipEvent>> {


        ValueState<OrderEvent> payState;
        ValueState<ReceipEvent> receipState;


        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState(new ValueStateDescriptor<>("pay-state", OrderEvent.class));
            receipState = getRuntimeContext().getState(new ValueStateDescriptor<>("receip-state", ReceipEvent.class));
        }

        @Override
        public void processElement1(OrderEvent orderEvent, CoProcessFunction<OrderEvent, ReceipEvent, Tuple2<OrderEvent, ReceipEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceipEvent>> collector) throws Exception {
            // 订单支付事件来了。判断是否已经有对应的到帐事件
            ReceipEvent receipEvent = receipState.value();
            if (receipEvent != null) {
                // 如果receipt不为空，说明到账事件已经来过，输出匹配事件，清空状态
                collector.collect(new Tuple2<>(orderEvent, receipEvent));
                payState.clear();
                receipState.clear();
            } else {
                // 如果没来 注册一个定时器，等待到账事件
                context.timerService().registerEventTimeTimer((orderEvent.getTimestamp() + 5)* 1000L);
                // 更新状态
                payState.update(orderEvent);
            }
        }

        @Override
        public void processElement2(ReceipEvent receipEvent, CoProcessFunction<OrderEvent, ReceipEvent, Tuple2<OrderEvent, ReceipEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceipEvent>> collector) throws Exception {
            // 到账事件来了，判断是否已经有对应的支付事件
            OrderEvent orderEvent = payState.value();
            if (orderEvent != null) {
                // 如果pay不为空，说明支付事件已经来过，输出匹配事件，清空状态
                collector.collect(new Tuple2<>(orderEvent, receipEvent));
                payState.clear();
                receipState.clear();
            } else {
                // 如果没来 注册一个定时器，等待支付事件
                context.timerService().registerEventTimeTimer((receipEvent.getTimestamp()+3) * 1000L);
                receipState.update(receipEvent);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<OrderEvent, ReceipEvent, Tuple2<OrderEvent, ReceipEvent>>.OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceipEvent>> out) throws Exception {
            // 定时器触发，有可能是有一个事件没来，不匹配，也有可能是都来过了，已经输出并清空状态
            // 判断哪个不为空，那么另一个就没来
            if (payState.value() != null) {
                ctx.output(unmatchedPays, payState.value());
            }
            if (receipState.value() != null) {
                ctx.output(unmatchedReceips, receipState.value());
            }
            payState.clear();
            receipState.clear();
        }
    }


}
