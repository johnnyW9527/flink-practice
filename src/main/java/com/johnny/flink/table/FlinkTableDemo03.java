package com.johnny.flink.table;

import com.johnny.flink.model.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/14 17:03
 */
public class FlinkTableDemo03 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Order> orderDs = env.addSource(new RichSourceFunction<Order>() {

            private Boolean isRunning = true;

            @Override
            public void run(SourceContext<Order> sourceContext) throws Exception {
                Random rand = new Random();
                while (isRunning) {
                    String orderId = UUID.randomUUID().toString();
                    int uId = rand.nextInt(3);
                    long orderPrice = rand.nextInt(101);
                    long eventTime = System.currentTimeMillis();
                    Order order = new Order(orderId, uId, orderPrice, eventTime);
                    TimeUnit.SECONDS.sleep(1);
                    sourceContext.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        SingleOutputStreamOperator<Order> waterDs = orderDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getCurrTime())
        );

        tEnv.createTemporaryView("t_order", waterDs, $("orderId"), $("uId"), $("orderPrice"), $("currTime").rowtime());

        tEnv.from("t_order").printSchema();

        Table resultTable = tEnv.from("t_order")
                .window(Tumble.over(lit(5).second())
                        .on($("currTime"))
                        .as("w"))
                .groupBy($("w"), $("uId"))
                .select(
                        $("uId"),
                        $("uId").count().as("totalCount"),
                        $("orderPrice").max().as("maxMoney"),
                        $("orderPrice").min().as("minMoney")
                );

        DataStream<Tuple2<Boolean, Row>> resultDs = tEnv.toRetractStream(resultTable, Row.class);
        resultDs.print();

        env.execute();

    }

}
