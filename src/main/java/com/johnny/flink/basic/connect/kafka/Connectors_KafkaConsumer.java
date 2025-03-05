package com.johnny.flink.basic.connect.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *需要设置如下参数:
 *  * 1.订阅的主题
 *  * 2.反序列化规则
 *  * 3.消费者属性-集群地址
 *  * 4.消费者属性-消费者组id(如果不设置,会有默认的,但是默认的不方便管理)
 *  * 5.消费者属性-offset重置规则,如earliest/latest...
 *  * 6.动态分区检测(当kafka的分区数变化/增加时,Flink能够检测到!)
 *  * 7.如果没有设置Checkpoint,那么可以设置自动提交offset,后续学习了Checkpoint会把offset随着做Checkpoint的时候提交到Checkpoint和默认主题中
 *  *
 * @author wan.liang(79274)
 * @date 2025/2/25 15:11
 */
public class Connectors_KafkaConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "node01:9092");
        prop.setProperty("group.id", "flink");
        prop.setProperty("auto.offset.reset", "latest");
        // 开启一个后台线程每隔5s检测一下kafka的分区情况
        prop.setProperty("flink.partition-discovery.interval-millis", "5000");
        prop.setProperty("enable.auto.commit", "true");
        prop.setProperty("auto.commit.interval.ms", "2000");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("topic_flink", new SimpleStringSchema(), prop);
        // 设置从记录的offset开始消费,如果没有记录从auto.offset.reset配置开始消费
        kafkaSource.setStartFromGroupOffsets();
        DataStreamSource<String> kafkaDs = env.addSource(kafkaSource);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDs = kafkaDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> groupDs = wordAndOneDs.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = groupDs.sum(1);

        result.print();
        env.execute();
    }

}
