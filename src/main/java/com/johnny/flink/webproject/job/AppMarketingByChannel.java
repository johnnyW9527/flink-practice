package com.johnny.flink.webproject.job;

import com.johnny.flink.webproject.function.SimulatedMarketingUserBehaviorSource;
import com.johnny.flink.webproject.model.ChannelPromotionCount;
import com.johnny.flink.webproject.model.MarketingUserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * <b>分渠道统计</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/25 20:16
 */
public class AppMarketingByChannel {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<MarketingUserBehavior> dataDs = env.addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<MarketingUserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));


        // 分渠道统计
        SingleOutputStreamOperator<ChannelPromotionCount> resultDs = dataDs.filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy(new KeySelector<MarketingUserBehavior, String>() {
                    @Override
                    public String getKey(MarketingUserBehavior value) throws Exception {
                        return value.getChannel() + "-" + value.getBehavior();
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new MarketingCountAgg(), new MarketingCountResult());

        resultDs.print();


        env.execute();
    }

    public static class MarketingCountResult extends ProcessWindowFunction<Long, ChannelPromotionCount, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Long, ChannelPromotionCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
            String[] split = s.split("-");
            Long count = iterable.iterator().next();
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            collector.collect(new ChannelPromotionCount(split[0], split[1], count, windowEnd));
        }
    }

    public static class MarketingCountAgg  implements AggregateFunction<MarketingUserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior value, Long accumulator) {
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

}
