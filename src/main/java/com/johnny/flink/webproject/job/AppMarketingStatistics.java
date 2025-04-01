package com.johnny.flink.webproject.job;

import com.johnny.flink.webproject.function.SimulatedMarketingUserBehaviorSource;
import com.johnny.flink.webproject.model.ChannelPromotionCount;
import com.johnny.flink.webproject.model.MarketingUserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * <b>不分渠道（总量）统计</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/25 20:25
 */
public class AppMarketingStatistics {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<MarketingUserBehavior> inputDs = env.addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<MarketingUserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));

        SingleOutputStreamOperator<ChannelPromotionCount> resultDs = inputDs.filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                        return new Tuple2<>("total", 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new MarketingStatisticsAgg(), new MarketingStatisticsResult());

        resultDs.print();

        env.execute();
    }

    public static class MarketingStatisticsAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class MarketingStatisticsResult implements WindowFunction<Long, ChannelPromotionCount, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
            String windowEnd = new Timestamp( timeWindow.getEnd() ).toString();
            Long count = iterable.iterator().next();
            collector.collect(new ChannelPromotionCount("total", "total", count, windowEnd));
        }
    }

}
