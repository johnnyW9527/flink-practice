package com.johnny.flink.action.function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 ** 自定义处理函数用来给超时订单做自动好评!
 *      * 如一个订单进来:<订单id, 2020-10-10 12:00:00>
 *      * 那么该订单应该在12:00:00 + 5s 的时候超时!
 *      * 所以我们可以在订单进来的时候设置一个定时器,在订单时间 + interval的时候触发!
 *      * KeyedProcessFunction<K, I, O>
 *      * KeyedProcessFunction<Tuple就是String, Tuple3<用户id, 订单id, 订单生成时间>, Object>
 * @author wan.liang(79274)
 * @date 2025/3/17 16:42
 */
public class TimerProcessFunction extends KeyedProcessFunction<String, Tuple3<String, String, Long>, String> {

    private final long interval;

    /**
     * key是订单号，value是订单完成时间
     */
    private MapState<String, Long> mapState;

    public TimerProcessFunction(long interval) {
        this.interval = interval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("mapState", String.class, Long.class);
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void processElement(Tuple3<String, String, Long> value, KeyedProcessFunction<String, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
        //处理每一个订单并设置定时器
        mapState.put(value.f1, value.f2);
        //如一个订单进来:<订单id, 2020-10-10 12:00:00>
        //那么该订单应该在12:00:00 + 5s 的时候超时!
        //在订单进来的时候设置一个定时器,在订单时间 + interval的时候触发!!!
        context.timerService().registerProcessingTimeTimer(value.f2 + interval);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple3<String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        //能够执行到这里说明订单超时了!超时了得去看看订单是否评价了(实际中应该要调用外部接口/方法查订单系统!,我们这里没有,所以模拟一下)
        //没有评价才给默认好评!并直接输出提示!
        //已经评价了,直接输出提示!
        Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            String orderId = entry.getKey();
            boolean result = isEvaluation(orderId);
            if (result) {
                out.collect("订单" + orderId + "已经评价过了!");
            } else {
                out.collect("订单" + orderId + "超时了!自动好评!");
            }
        }
    }

    private boolean isEvaluation(String orderId) {
        return orderId.hashCode() % 2 == 0;
    }
}
