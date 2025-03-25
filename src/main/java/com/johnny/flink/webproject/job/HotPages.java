package com.johnny.flink.webproject.job;

import com.google.common.collect.Lists;
import com.johnny.flink.webproject.model.ApacheLogEvent;
import com.johnny.flink.webproject.model.PageViewCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * <b>热门页面</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/24 19:29
 */
public class HotPages {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.获取数据源
        DataStreamSource<String> inputDs = env.readTextFile("D:\\idea_workspace\\flink-practice\\flink-practice\\src\\main\\resources\\temp\\apache.log");

        // 2.格式化数据，以及添加水位线
        SingleOutputStreamOperator<ApacheLogEvent> mapDs = inputDs.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timeStamp = dateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timeStamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));


        // 3.分组开窗聚合
        // 3.1 输出侧输出流，用于处理迟到数据
        // 3.2 分组开窗聚合
        OutputTag<ApacheLogEvent> lateDataTag = new OutputTag<ApacheLogEvent>("late-data") {
        };

        SingleOutputStreamOperator<PageViewCount> aggDs = mapDs.filter(data -> "GET".equals(data.getMethod()))
                .filter(data -> {
                    String regex = "^((?!\\\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateDataTag)
                .aggregate(new PageCountAgg(), new PageCountResult());


        // 收集同一窗口count数据，排序输出
        SingleOutputStreamOperator<String> processDs = aggDs.keyBy(PageViewCount::getWindowEnd)
                .process(new HotPagesProcess(3));

        processDs.print("keyed");

        env.execute();

    }

    public static class HotPagesProcess extends KeyedProcessFunction<Long, PageViewCount, String> {

        MapState<String, Long> countState;

        private final Integer topSize;

        public HotPagesProcess(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("mapState", String.class, Long.class);
            countState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void processElement(PageViewCount pageViewCount, KeyedProcessFunction<Long, PageViewCount, String>.Context context, Collector<String> collector) throws Exception {
            countState.put(pageViewCount.getUrl(), pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
            // 注册有一个1分钟之后的定时器，用来清空状态
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, PageViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 先判断是否到窗口的关闭清理时间，如果是，直接清空状态返回
            if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
                countState.clear();
                return;
            }
            ArrayList<Map.Entry<String, Long>> entries = Lists.newArrayList(countState.entries());

            entries.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
            // 格式化成String输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("=================================================\n");
            resultBuilder.append("窗口结束时间:").append(new Timestamp(timestamp -1)).append("\n");
            // 遍历
            for (int i = 0; i < Math.min(topSize, entries.size()); i++){
                Map.Entry<String, Long> currentItemViewCount = entries.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(currentItemViewCount.getKey())
                        .append(" 浏览量 = ").append(currentItemViewCount.getValue())
                        .append("\n");
            }
            resultBuilder.append("======================================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }


    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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

   public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {

       @Override
       public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
           collector.collect(new PageViewCount(s, timeWindow.getEnd(), iterable.iterator().next()));
       }
   }
}
