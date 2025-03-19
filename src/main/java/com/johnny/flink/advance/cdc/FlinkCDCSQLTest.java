package com.johnny.flink.advance.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/19 15:54
 */
public class FlinkCDCSQLTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SourceFunction<JSONObject> sourceFunction = MySQLSource.<JSONObject>builder()
                .hostname("10.31.1.122")
                .port(3306)
                .databaseList("cdc_test") // monitor all tables under inventory database

                .username("root")
                //.password("abc123")
                .password("Abc123456!")
                .deserializer(new CdcDwdDeserializationSchema()) // converts SourceRecord to String
                .build();
        DataStreamSource<JSONObject> stringDataStreamSource = env.addSource(sourceFunction);

        stringDataStreamSource.print("===>");
        try {


            env.execute("测试mysql-cdc");
        } catch (Exception e) {


            e.printStackTrace();
        }

    }
}
