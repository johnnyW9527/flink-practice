package com.johnny.flink.webproject.job;

import com.johnny.flink.webproject.model.OrderEvent;
import com.johnny.flink.webproject.model.ReceipEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.time.Duration;

/**
 * <b>账单核对-使用join</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/4/1 14:09
 */
public class TxPayMatchByJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        URL orderResource = TxPayMatchByJoin.class.getResource("/temp/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> inputDs = env.readTextFile(orderResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L))
                .filter(data -> !"".equals(data.getTxId()));

        URL receiptResource = TxPayMatchByJoin.class.getResource("/temp/ReceiptLog.csv");

        SingleOutputStreamOperator<ReceipEvent> receipDs = env.readTextFile(receiptResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceipEvent(fields[0], fields[1], new Long(fields[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ReceipEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L));

        inputDs.keyBy(OrderEvent::getTxId)
                        .intervalJoin(receipDs.keyBy(ReceipEvent::getTxId))
                        .between(Time.seconds(-3), Time.seconds(5))
                        .process(new TxPayMatchDetectByJoin())
                        .print();

        env.execute();
    }

    public static class TxPayMatchDetectByJoin extends ProcessJoinFunction<OrderEvent, ReceipEvent, Tuple2<OrderEvent, ReceipEvent>> {
        @Override
        public void processElement(OrderEvent orderEvent, ReceipEvent receipEvent, ProcessJoinFunction<OrderEvent, ReceipEvent, Tuple2<OrderEvent, ReceipEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceipEvent>> collector) throws Exception {
            collector.collect(new Tuple2<>(orderEvent, receipEvent));
        }
    }

}
