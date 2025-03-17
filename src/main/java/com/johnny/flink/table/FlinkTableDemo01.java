package com.johnny.flink.table;

import com.johnny.flink.model.ProductOrder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/14 15:37
 */
public class FlinkTableDemo01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<ProductOrder> orderA = env.fromCollection(Arrays.asList(
                new ProductOrder(1L, "apple", 2),
                new ProductOrder(1L, "diaper", 4),
                new ProductOrder(3L, "rubber", 2)
        ));
        DataStreamSource<ProductOrder> orderB = env.fromCollection(Arrays.asList(
                new ProductOrder(2L, "pen", 3),
                new ProductOrder(2L, "rubber", 3),
                new ProductOrder(4L, "beer", 4)
        ));

        // 注册表
        Table tableA = tEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        tEnv.createTemporaryView("OrderB", orderB, $("user"), $("product"), $("amount"));

        Table resultTable = tEnv.sqlQuery(
                "SELECT * FROM " + tableA + " WHERE amount > 2  UNION ALL SELECT * FROM OrderB WHERE amount < 2"
        );



        //5.输出结果
        DataStream<ProductOrder> resultDS = tEnv.toAppendStream(resultTable, ProductOrder.class);
        resultDS.print();

        env.execute();

    }

}
