package com.johnny.flink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
 * @date 2025/3/14 16:29
 */
public class FlinkTableDemo02 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<FlinkSqlDemo01.WC> input = env.fromElements(
                new FlinkSqlDemo01.WC("Hello", 1),
                new FlinkSqlDemo01.WC("World", 1),
                new FlinkSqlDemo01.WC("Hello", 1)
        );

        Table table = tEnv.fromDataStream(input);
        Table resultTable = table.groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"))
                .filter($("frequency").isEqual(2));
        DataStream<Tuple2<Boolean, FlinkSqlDemo01.WC>> retractStream =
                tEnv.toRetractStream(resultTable, FlinkSqlDemo01.WC.class);

        retractStream.print();

        env.execute();

    }

}
