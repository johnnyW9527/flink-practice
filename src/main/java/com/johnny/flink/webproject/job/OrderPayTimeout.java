package com.johnny.flink.webproject.job;

import com.johnny.flink.webproject.model.OrderEvent;
import com.johnny.flink.webproject.model.OrderResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/4/1 10:40
 */
public class OrderPayTimeout {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        URL resource = OrderPayTimeout.class.getResource("/temp/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> inputDs = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L));

        // 定义一个带时间限制的模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent> (){

            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));


        // 定义侧输出流标签，用来表示超时事件
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};

        // 将pattern应用到输入数据流上，得到pattern
        PatternStream<OrderEvent> patternDs = CEP.pattern(inputDs.keyBy(OrderEvent::getOrderId), orderPayPattern);

        SingleOutputStreamOperator<OrderResult> resultDs = patternDs.select(orderTimeoutTag, new OrderTimeoutSelect(), new OrderPaySelect());
        resultDs.print("payed  normally");
        resultDs.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute();
    }

    /**
     * 实现自定义的超时事件处理函数
     */
    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
            Long timeoutOrderId = map.get("create").iterator().next().getOrderId();
            return new OrderResult(timeoutOrderId, "timeout" + l);
        }
    }

    /**
     * 实现自定义的正常匹配事件处理函数
     */
    public static class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            Long payOrderId = map.get("pay").iterator().next().getOrderId();
            return new OrderResult(payOrderId, "payed");
        }
    }

}
