package com.johnny.flink.webproject.job;

import com.google.common.collect.Lists;
import com.johnny.flink.webproject.model.ItemViewCount;
import com.johnny.flink.webproject.model.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;

/**
 * <b>统计近1小时内的热门商品，每5分钟更新一次</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/24 11:02
 */
public class HotItems {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDs = env.readTextFile("D:\\idea_workspace\\flink-practice\\flink-practice\\src\\main\\resources\\temp\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> mapDs = inputDs.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], Long.parseLong(fields[4]));
        })
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L));

        SingleOutputStreamOperator<ItemViewCount> windowResultDs = mapDs.keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new CountAgg(), new WindowResultFunction());

        SingleOutputStreamOperator<String> resultDs = windowResultDs.keyBy(ItemViewCount::getWindowEnd)
                .process(new TopNHotItems(5));


        resultDs.print();

        env.execute();
    }

    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {


        private final Integer topSize;

        ListState<ItemViewCount>  itemViewCountListState;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<ItemViewCount> itemViewCountListStateDescriptor = new ListStateDescriptor<>("itemViewCountListState", ItemViewCount.class);
            itemViewCountListState = getRuntimeContext().getListState(itemViewCountListStateDescriptor);
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, KeyedProcessFunction<Long, ItemViewCount, String>.Context context, Collector<String> collector) throws Exception {

            itemViewCountListState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get());
            itemViewCounts.sort((o1, o2) -> o2.getCount().intValue() - o1.getCount().intValue());

            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append( new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for( int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++ ){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }


    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(Long itemId, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            Long countValue = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId, timeWindow.getEnd(), countValue));

        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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
