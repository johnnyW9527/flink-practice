package com.johnny.flink.webproject.job;

import com.johnny.flink.webproject.model.PageViewCount;
import com.johnny.flink.webproject.model.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;

/**
 * <b>页面独立访问量</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/25 09:45
 */
public class UniqueVisitor {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> inputDs = env.readTextFile("D:\\idea_workspace\\flink-practice\\flink-practice\\src\\main\\resources\\temp\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> mapDs = inputDs.map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], Long.parseLong(fields[4]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L));

        SingleOutputStreamOperator<PageViewCount> resultDs = mapDs.filter(data -> "pv".equals(data.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(5)))
                .apply(new UvCountResult());

        resultDs.print();

        env.execute();
    }


    public static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        @Override
        public void apply(TimeWindow timeWindow, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            HashSet<Long> uvSet = new HashSet<>();
            for (UserBehavior userBehavior : iterable) {
                uvSet.add(userBehavior.getUserId());
            }
            collector.collect(new PageViewCount("uv", timeWindow.getEnd(), (long) uvSet.size()));
        }
    }


}
