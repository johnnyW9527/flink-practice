package com.johnny.flink.webproject.job;

import com.johnny.flink.webproject.model.AdClickEvent;
import com.johnny.flink.webproject.model.AdCountViewByProvince;
import com.johnny.flink.webproject.model.BlackListUserWarning;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
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
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;

 /**
 * <b>黑名单过滤</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/25 20:34
 */
public class AdStatisticsByProvince {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> adClickDs = env.readTextFile("D:\\idea_workspace\\flink-practice\\flink-practice\\src\\main\\resources\\temp\\AdClickLog.csv");

        SingleOutputStreamOperator<AdClickEvent> mapDs = adClickDs.map(line -> {
            String[] fields = line.split(",");
            return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));

        // 对同一个用户点击同一个广告的行为进行检测预警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = mapDs.keyBy(new KeySelector<AdClickEvent, String>() {
            @Override
            public String getKey(AdClickEvent value) throws Exception {
                return value.getUserId() + "-" + value.getAdId();
            }
        }).process(new FilterBlackListUser(100));

        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultDs = filterAdClickStream.keyBy(AdClickEvent::getProvince)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountResultDs.print();

        filterAdClickStream.getSideOutput(new OutputTag<BlackListUserWarning>("blacklist") {})
                .print("blacklist-user");

        env.execute();
    }

    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {
            String windowEnd = new Timestamp( window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new AdCountViewByProvince(s, count, windowEnd));
        }
    }


    public static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
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

    public static class FilterBlackListUser extends KeyedProcessFunction<String, AdClickEvent, AdClickEvent> {

        /**
         * 点击次数上限
         */
        private final Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }


        /**
         * 保存当前用户对某一广告的点击次数
         */
        ValueState<Long> countState;

        /**
         * 保存当前用户是否已经被发送到黑名单
         */
        ValueState<Boolean> isScanState;


        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count-state", Long.class));
            isScanState = getRuntimeContext().getState(new ValueStateDescriptor<>("is-scan-state", Boolean.class));
        }

        @Override
        public void processElement(AdClickEvent adClickEvent, KeyedProcessFunction<String, AdClickEvent, AdClickEvent>.Context context, Collector<AdClickEvent> collector) throws Exception {
            // 判断当前用户对同一广告的点击次数，如果不够上限，就count+1 正常输出，如果达到上限，直接过滤掉，并侧输出流出黑名单告警
            long curCount = countState.value() == null ? 0L : countState.value();
            boolean isFlag = isScanState.value() != null && isScanState.value();
            // 判断是否是第一个数据，如果是的话 ，注册一个第二天0点定时器，用来定时触发
            if (curCount == 0) {
                long ts = (context.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000;
                context.timerService().registerProcessingTimeTimer(ts);
            }
            // 判断是否报警
            if (curCount >= countUpperBound){
                // 判断是否输出到黑名单过，如果没有的话就输出到侧输出流
                if (!isFlag){
                    isScanState.update(true);       //  更新状态
                    context.output( new OutputTag<BlackListUserWarning>("blacklist"){},
                            new BlackListUserWarning(adClickEvent.getUserId(), adClickEvent.getAdId(), "click over " + countUpperBound + "times." ));
                    }
                    return;
            }
            // 如果没有返回，点击次数加1，更新状态，正常输出到数据主流
            countState.update(curCount + 1);
            collector.collect(adClickEvent);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, AdClickEvent, AdClickEvent>.OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            // 清空所有状态
            countState.clear();
            isScanState.clear();
        }
    }

}
