package com.johnny.flink.basic.connect.redis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 19:38
 */
public class Connectors_Redis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineDs = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDs =
                lineDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> groupsDs = wordAndOneDs.keyBy(t -> t.f0);
        //3.3聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = groupsDs.sum(1);

        //4.Sink
        result.print();

        //-1.创建RedisSink之前需要创建RedisConfig
        //连接单机版Redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        //连接集群版Redis
        //HashSet<InetSocketAddress> nodes = new HashSet<>(Arrays.asList(new InetSocketAddress(InetAddress.getByName("node1"), 6379),new InetSocketAddress(InetAddress.getByName("node2"), 6379),new InetSocketAddress(InetAddress.getByName("node3"), 6379)));
        //FlinkJedisClusterConfig conf2 = new FlinkJedisClusterConfig.Builder().setNodes(nodes).build();
        //连接哨兵版Redis
        //Set<String> sentinels = new HashSet<>(Arrays.asList("node1:26379", "node2:26379", "node3:26379"));
        //FlinkJedisSentinelConfig conf3 = new FlinkJedisSentinelConfig.Builder().setMasterName("mymaster").setSentinels(sentinels).build();

        //-3.创建并使用RedisSink
        result.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisWordCountMapper()));

        //5.execute
        env.execute();
    }

    /**
     * -2.定义一个Mapper用来指定存储到Redis中的数据结构
     */
    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "WordCount");
        }
        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }
        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }

}
