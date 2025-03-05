package com.johnny.flink.utils;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
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
 * @date 2025/2/25 19:22
 */
public class KafkaUtil {

    public static Logger log = LoggerFactory.getLogger(KafkaUtil.class);

    /**
     * 获取Kafka消费者的属性配置
     * 此方法用于初始化Kafka消费者所需的属性设置，包括集群地址、消费者组ID以及其他消费配置
     *
     * @param groupId 消费者组的唯一标识符，用于区分不同的消费者组
     * @return Properties 包含了所有Kafka消费者属性的Properties对象
     */
    public static Properties getKafkaProperties(String groupId) {
        // Kafka的参数设置
        Properties props = new Properties();

        // 集群地址 和 消费者组id（最基础的配置，必须要有）
        // 这里需要设置Kafka集群的地址，多个地址之间用逗号分隔
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        // 设置消费者组ID，用于标识一组消费者
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 开启 FlinkKafkaConsumer 的自动分区检测，用于检测kafka中topic的分区变更
        // 设置分区检测间隔时间，单位为毫秒。如果不需要自动检测分区变化，可以不设置此参数
        props.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "");

        // 偏移量自动提交，当checkpoint关闭时会启动此参数，当checkpoint开启时，并设置了setCommitOffsetsOnCheckpoints(true)（此参数默认为true）时，会根据checkpoint的时间向kafka.broker中提交offset
        // 设置是否启用自动提交偏移量
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "");
        // 设置自动提交偏移量的间隔时间，单位为毫秒
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "");

        // 设置kafka消费者的事务级别
        // 可以选择"read_uncommitted"或"read_committed"，影响可见性和性能
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "");

        // 当在 FlinkKafkaConsumer 中没有设置消费级别，并在checkpoint中没有偏移量时，使用此设置来消费kafka中的数据
        // 具体意义：当在kafka中保存偏移量的topic中有偏移量时从偏移量消费，没有从最新开始消费（其他还可以设置earliest，从最开始的数据开始消费等）
        // 一般情况下，会直接在 FlinkKafkaConsumer 中设置消费属性
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "");

        // 返回参数设置对象
        return props;
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topicName, String groupId) {
        Properties properties = KafkaUtil.getKafkaProperties(groupId);

        return new FlinkKafkaConsumer<String>(topicName, new SimpleStringSchema(), properties);
    }

    /**
     * 获取Kafka消费者对象
     *
     * @param topicName  要消费的Kafka主题名称
     * @param groupId    消费者组ID
     * @param timestamp  消费开始的时间戳
     * @return           返回配置好的FlinkKafkaConsumer对象
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topicName, String groupId, Long timestamp) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaProperties(groupId);

        // 创建一个FlinkKafka的消费者
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), props);

        // 设置从指定时间戳开始消费
        log.info("从kafka的指定时间戳开始消费，时间戳：" + timestamp);
        consumer.setStartFromTimestamp(timestamp);

        // 返回消费者对象
        return consumer;

    }

    /**
     * 创建一个包含时间戳的Flink Kafka消费者
     * 该方法用于生成一个Flink Kafka消费者实例，该实例能够从指定主题中消费消息，并提取消息中的时间戳
     *
     * @param topicName  要消费的Kafka主题名称
     * @param groupId    消费者组ID，用于Kafka的消费者组机制
     * @return 返回一个Flink Kafka消费者实例，能够消费字符串消息和对应的时间戳
     */
    public static FlinkKafkaConsumer<Tuple2<String, Long>> getKafkaConsumerContainTimestamp(String topicName, String groupId) {
        // 获取Kafka属性配置，包括bootstrap.servers等基本配置
        Properties properties = KafkaUtil.getKafkaProperties(groupId);

        // 定义一个Kafka反序列化方案，用于将Kafka中的消息转换为Flink能够处理的数据类型
        KafkaDeserializationSchema<Tuple2<String, Long>> deserializationSchema = new KafkaDeserializationSchema<Tuple2<String, Long>>() {

            /**
             * 获取此反序列化方案生成的数据类型信息
             *
             * @return 返回数据类型信息，这里是一个Tuple2，包含字符串和长整型
             */
            @Override
            public TypeInformation<Tuple2<String, Long>> getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                });
            }

            /**
             * 判断是否达到数据流的末尾
             * 对于Kafka消费者来说，这通常返回false，因为Kafka是一个持续的数据流
             *
             * @param stringLongTuple2 消费的消息
             * @return 总是返回false，表示数据流不会结束
             */
            @Override
            public boolean isEndOfStream(Tuple2<String, Long> stringLongTuple2) {
                return false;
            }

            /**
             * 反序列化Kafka中的消息记录
             * 将消费者记录的值转换为字符串，并提取记录的时间戳，然后将两者封装成一个Tuple2
             *
             * @param consumerRecord 消费的Kafka记录
             * @return 返回一个Tuple2，包含消息内容和时间戳
             * @throws Exception 如果反序列化过程中出现错误，抛出异常
             */
            @Override
            public Tuple2<String, Long> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                String message = new String(consumerRecord.value(), StandardCharsets.UTF_8);
                long timestamp = consumerRecord.timestamp();
                return Tuple2.of(message, timestamp);
            }
        };
        // 返回一个新的Flink Kafka消费者实例，使用指定的主题、反序列化方案和属性配置
        return new FlinkKafkaConsumer<Tuple2<String, Long>>(topicName, deserializationSchema, properties);
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topicName) {
        return new FlinkKafkaProducer<>("", topicName, new SimpleStringSchema());
    }

    /**
     * 获取具有精确一次语义的 Kafka 生产者
     * 精确一次语义确保每个记录正好被发送一次到 Kafka，即使在故障情况下也不会重复或丢失
     *
     * @param serializationSchema 序列化方案，定义如何将输入数据转换为 Kafka 消息
     * @param defaultTopicName 默认的 Kafka 主题名称，如果未在序列化方案中指定主题，则使用此主题
     * @return 返回配置了精确一次语义的 FlinkKafkaProducer 实例
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducerExactlyOnce(KafkaSerializationSchema<T> serializationSchema, String defaultTopicName) {

        Properties prop = new Properties();
        // kafka的 bootstrap.servers
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        //如果 10 分钟没有更新状态，则超时( 默认超时时间是1分钟)，表示已经提交事务到kafka，但10分钟还没有上传数据，结束事务
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(10 * 60 * 1000));
        // 配置生产者的kafka的单条消息的最大大小（ 默认为1M，这里设置为10M ）
        prop.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(10 * 1000 * 1000));

        // 创建并返回具有精确一次语义的 Flink Kafka 生产者
        return new FlinkKafkaProducer<>(defaultTopicName, serializationSchema, prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

    }

    public static <T extends String> FlinkKafkaProducer<T> getKafkaProducerExactlyOnce(String topicName) {

        return KafkaUtil.getKafkaProducerExactlyOnce(
                new KafkaSerializationSchema<T>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(T t, @Nullable Long aLong) {
                        return new ProducerRecord<>(topicName, JSON.toJSONBytes(t));
                    }
                },
                ""
        );

    }



}
