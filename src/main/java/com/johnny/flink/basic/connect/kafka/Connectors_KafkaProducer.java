package com.johnny.flink.basic.connect.kafka;

import com.alibaba.fastjson.JSON;
import com.johnny.flink.model.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 19:14
 */
public class Connectors_KafkaProducer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> stuDs = env.fromElements(new Student(1, "Johnny", 18));

        SingleOutputStreamOperator<String> jsonDs = stuDs.map(new MapFunction<Student, String>() {
            @Override
            public String map(Student student) throws Exception {
                return JSON.toJSONString(student);
            }
        });

        jsonDs.print();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "node01:9092");
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>("topic_flink", new SimpleStringSchema(), props);

        jsonDs.addSink(kafkaSink);

        env.execute();

    }
}
