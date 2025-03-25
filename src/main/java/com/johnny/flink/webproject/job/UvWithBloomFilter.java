package com.johnny.flink.webproject.job;

import com.johnny.flink.webproject.model.PageViewCount;
import com.johnny.flink.webproject.model.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

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
 * @date 2025/3/25 10:54
 */
public class UvWithBloomFilter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> inputDs = env.readTextFile("D:\\idea_workspace\\flink-practice\\flink-practice\\src\\main\\resources\\temp\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> mapDs = inputDs.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], Long.parseLong(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L));

        SingleOutputStreamOperator<PageViewCount> resultDs = mapDs.filter(data -> "pv".equals(data.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFilter());

        resultDs.print();

        env.execute();
    }

    public static class UvCountResultWithBloomFilter extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        Jedis jedis ;
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            // 要处理1亿个数据，用64MB大小的位图
            myBloomFilter = new MyBloomFilter(1 << 29);
        }

        @Override
        public void process(ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>.Context context, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            // 将位图和窗口count值全部存入redis，用windowEnd作为key
            long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd + "";
            // 把count值存成一张hash表
            String countHashName = "uv_count";
            String countKey = windowEnd + "";

            // 1. 取当前的userId
            long userId = iterable.iterator().next().getUserId();

            // 2. 计算位图中的offset
            Long offset = myBloomFilter.hashCode(userId + "", 61);
            // 3. 判断位图中的offset位置是否为1，如果为1，说明已经存在，直接返回；如果不为1，说明不存在，则进行count++，并设置位图中的offset位置为1
            Boolean isExist = jedis.getbit(bitmapKey, offset);
            if (!isExist) {
                jedis.setbit(bitmapKey, offset, true);
                // 更新redis保存的count值
                long uvCount = 0L;
                String uvCountStr = jedis.hget(countHashName, countKey);
                if (uvCountStr != null && !uvCountStr.isEmpty()) {
                    uvCount = Long.parseLong(uvCountStr);
                }
                jedis.hset(countHashName, countKey, uvCount + 1 + "");
                collector.collect(new PageViewCount("uv", windowEnd, Long.parseLong(jedis.hget(countHashName, countKey))));
            }

        }
    }

    public static class MyBloomFilter {

        // 定义位图的大小，一般需要定义为2的整次幂
        private final Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        public Long hashCode(String value, Integer seed) {
            long result = 0L;
            for (int v = 0; v < value.length(); v++) {
                result = result * seed + value.charAt(v);
            }
            return result & (cap - 1);
        }


    }


    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior userBehavior, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            // 每一条数据来到，直接触发窗口计算，并直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }

}
