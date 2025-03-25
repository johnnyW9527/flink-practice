package com.johnny.flink.webproject.job;

import com.johnny.flink.webproject.model.PageViewCount;
import com.johnny.flink.webproject.model.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Random;

/**
 * <b>页面访问量</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/24 20:10
 */
public class PageView {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. 获取数据源
        DataStreamSource<String> inputDs = env.readTextFile("D:\\idea_workspace\\flink-practice\\flink-practice\\src\\main\\resources\\temp\\UserBehavior.csv");

        // 2. 格式化数据，以及添加水位线
        SingleOutputStreamOperator<UserBehavior> mapDs = inputDs.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], Long.parseLong(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));

        // 3. 分组、开窗、聚合
//        SingleOutputStreamOperator<Tuple2<String, Long>> pvResultDs = mapDs.filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
//                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
//                        return Tuple2.of("pv", 1L);
//                    }
//                })
//                .keyBy(tuple2 -> tuple2.f0)
//                .window(TumblingEventTimeWindows.of(Time.hours(1)))
//                .sum(1);

        // 并行任务，设计随机key，解决数据倾斜问题
        SingleOutputStreamOperator<PageViewCount> aggDs = mapDs.filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                }).keyBy(new KeySelector<Tuple2<Integer, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<Integer, Long> value) throws Exception {
                        return value.f0 + "";
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PvCountAgg(), new PvCountResult());

        SingleOutputStreamOperator<PageViewCount> resultDs = aggDs.keyBy(PageViewCount::getWindowEnd)
                .process(new TotalPvCount());

        resultDs.print();


        env.execute();

    }

    public static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {


        private ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("value-state", Long.class, 0L);
            valueState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void processElement(PageViewCount pageViewCount, KeyedProcessFunction<Long, PageViewCount, PageViewCount>.Context context, Collector<PageViewCount> collector) throws Exception {
            valueState.update(valueState.value() + pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, PageViewCount, PageViewCount>.OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 从定时器触发，所有分组count值都到齐，直接输出当前的总count值
            Long value = valueState.value();
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), value));
            // 清空状态
            valueState.clear();
        }
    }

    public static class PvCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(tuple, timeWindow.getEnd(), iterable.iterator().next()));
        }
    }



    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
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
