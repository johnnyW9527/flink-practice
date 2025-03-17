package com.johnny.flink.action;

import com.johnny.flink.action.function.TimerProcessFunction;
import com.johnny.flink.action.source.OrderSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *在电商领域会有这么一个场景，如果用户买了商品，在订单完成之后，一定时间之内没有做出评价，系统自动给与五星好评，
 *我们今天主要使用Flink的定时器来简单实现这一功能
 * @author wan.liang(79274)
 * @date 2025/3/17 16:30
 */
public class OrderAutomaticFavorableComments {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple3<String, String, Long>> sourceDs = env.addSource(new OrderSource());


        long interval = 5000L;

        SingleOutputStreamOperator<String> process = sourceDs.keyBy(f -> f.f0)
                .process(new TimerProcessFunction(interval));

        process.print();

        env.execute();


    }

}
