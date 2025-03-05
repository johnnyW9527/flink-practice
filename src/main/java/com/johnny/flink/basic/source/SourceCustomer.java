package com.johnny.flink.basic.source;

import com.johnny.flink.model.Order;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 10:34
 */
public class SourceCustomer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Order> customDs = env.addSource(new MySource());


        customDs.print();

        env.execute();

    }

    static class MySource extends RichParallelSourceFunction<Order> {


        private Boolean isRunning = true;

        @Override
        public void run(SourceContext<Order> sourceContext) throws Exception {
            Random rand = new Random();
            while (isRunning) {
                Thread.sleep(1000);
                String orderId = UUID.randomUUID().toString();
                int uId = rand.nextInt(3);
                long orderPrice = rand.nextInt(101);
                sourceContext.collect(new Order(orderId, uId, orderPrice, System.currentTimeMillis()));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
