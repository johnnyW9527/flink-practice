package com.johnny.flink.utils;

import com.alibaba.fastjson.JSON;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

    public static Logger logger = LoggerFactory.getLogger(KafkaUtil.class);

    /**
     * 获取Kafka消费者属性配置
     *
     * @param groupId 消费者组ID，用于区分不同的消费者组
     * @return Properties对象，包含了Kafka消费者的配置信息
     */
    public static Properties getKafkaConsumerProperties(String groupId) {

        // 创建Properties对象以存储Kafka消费者配置
        Properties props = new Properties();

        // 设置Kafka集群地址和消费者组ID
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ModelUtil.getConfigValue("bootstrap.servers"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 配置偏移量自动提交设置，包括自动提交的启用和间隔时间
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ModelUtil.getConfigValue("enable.auto.commit"));
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, ModelUtil.getConfigValue("auto.commit.interval.ms"));

        // 设置事务的提交发送，以确保消费者只读取已提交的消息
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // 配置分区发现间隔，开启后台线程定期检测Kafka分区情况
        props.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, ModelUtil.getConfigValue("flink.partition-discovery.interval-millis"));

        // 设置自动偏移量重置策略，决定如何从Kafka主题中消费消息
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ModelUtil.getConfigValue("auto.offset.reset"));

        // 返回配置好的Properties对象
        return props;
    }



    /**
     * 获取Kafka生产者的消息发送配置属性
     * 此方法用于初始化和返回一个包含Kafka生产者配置属性的Properties对象
     * 它配置了生产者与Kafka集群通信所需的基本设置，如服务器地址、事务超时时间、消息大小限制等
     *
     * @return Properties 包含Kafka生产者配置属性的Properties对象
     */
    public static Properties getKafkaProducerProperties() {

        // 创建一个Properties对象来存储配置属性
        Properties props = new Properties();

        // 设置Kafka集群的地址和端口
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ModelUtil.getConfigValue("bootstrap.servers"));

        // 设置事务超时时间，如果10分钟内没有更新状态，则认为事务已经结束
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(10 * 60 * 1000));

        // 设置生产者可以发送的最大消息大小
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(10 * 1024 * 1024));

        // 设置消息的压缩类型，以提高传输效率
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // 设置批处理的大小，以优化网络传输效率
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(8 * 1024));

        // 设置生产者可用的总内存空间，用于缓冲区
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(64 * 1024 * 1024));

        // 返回配置属性对象
        return props;
    }


    /**
     * 创建并返回一个Flink Kafka消费者
     * 该方法用于初始化并返回一个配置好的Flink Kafka消费者对象，用于消费指定主题的消息
     *
     * @param topicName  需要消费的Kafka主题名称
     * @param groupId    消费者组ID，用于区分不同的消费者组
     * @return           返回一个配置好的Flink Kafka消费者对象
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topicName, String groupId) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // 创建一个FlinkKafka的消费者
        return new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), props);

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
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // 创建一个FlinkKafka的消费者
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), props);

        // 设置从指定时间戳开始消费
        logger.info(">>>>> 从kafka的指定时间戳开始消费，时间戳：" + timestamp);
        consumer.setStartFromTimestamp(timestamp);

        // 返回消费者对象
        return consumer;

    }



    /**
     * 获取基于Apache Flink的Kafka消费者，该消费者使用Avro模式进行数据反序列化
     *
     * @param topicName Kafka主题名称，消费者将订阅该主题
     * @param groupId 消费者组ID，用于Kafka消费者组管理
     * @param clz 消费者将产生的数据类型，即Avro反序列化后的对象类型
     * @return 返回一个配置好的FlinkKafkaConsumer实例，用于消费Kafka主题中的消息
     */
    public static <T> FlinkKafkaConsumer<T> getKafkaConsumerAvro(String topicName, String groupId, Class<T> clz) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // kafka反序列化对象
        KafkaDeserializationSchema<T> deserializationSchema = new KafkaDeserializationSchema<T>() {
            @Override
            public TypeInformation<T> getProducedType() {
                return TypeInformation.of(clz);
            }

            @Override
            public boolean isEndOfStream(T nextElement) {
                return false;
            }

            @Override
            public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                ReflectDatumReader<T> reflectDatumReader = new ReflectDatumReader<>(ReflectData.AllowNull.get().getSchema(clz));
                return reflectDatumReader.read(null, DecoderFactory.get().binaryDecoder(record.value(), null));
            }
        };

        // 创建基于flink的kafka消费者
        return new FlinkKafkaConsumer<T>(topicName, deserializationSchema, props);

    }


    /**
     * 获取基于Apache Flink的Kafka消费者，该消费者使用Avro模式进行数据反序列化
     *
     * @param topicName Kafka主题名称，消费者将从该主题消费数据
     * @param groupId 消费者组ID，用于Kafka消费者配置
     * @param clz 消费的数据类型，用于反序列化Kafka消息
     * @param timestamp 指定消费者开始消费的消息的时间戳
     * @return 返回一个配置好的FlinkKafkaConsumer实例
     */
    public static <T> FlinkKafkaConsumer<T> getKafkaConsumerAvro(String topicName, String groupId, Class<T> clz, Long timestamp) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // kafka反序列化对象
        KafkaDeserializationSchema<T> deserializationSchema = new KafkaDeserializationSchema<T>() {
            @Override
            public TypeInformation<T> getProducedType() {
                return TypeInformation.of(clz);
            }

            @Override
            public boolean isEndOfStream(T nextElement) {
                return false;
            }

            @Override
            public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                ReflectDatumReader<T> reflectDatumReader = new ReflectDatumReader<>(ReflectData.AllowNull.get().getSchema(clz));
                return reflectDatumReader.read(null, DecoderFactory.get().binaryDecoder(record.value(), null));
            }
        };

        // 创建基于flink的kafka消费者
        FlinkKafkaConsumer<T> consumer = new FlinkKafkaConsumer<>(topicName, deserializationSchema, props);

        // 设置从指定时间戳开始消费
        logger.info(">>>>> 从kafka的指定时间戳开始消费，时间戳：" + timestamp);
        consumer.setStartFromTimestamp(timestamp);

        return consumer;

    }


    /**
     * 获取带有时间戳的Kafka消费者
     * 该方法用于创建一个Flink Kafka消费者，能够消费Kafka主题中的消息，并提取消息的时间戳
     * 主要用于需要处理消息产生时间的场景
     *
     * @param topicName 消费的Kafka主题名称
     * @param groupId 消费者组ID，用于Kafka消费者配置
     * @return 返回一个Flink Kafka消费者对象，能够消费主题中的消息，并附带消息的时间戳
     */
    public static FlinkKafkaConsumer<Tuple2<String, Long>> getKafkaConsumerAndTimestamp(String topicName, String groupId) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // 自定义kafka的反序列化器
        KafkaDeserializationSchema<Tuple2<String, Long>> deserializationSchema = new KafkaDeserializationSchema<Tuple2<String, Long>>() {

            @Override
            public TypeInformation<Tuple2<String, Long>> getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                });
            }

            @Override
            public boolean isEndOfStream(Tuple2<String, Long> nextElement) {
                return false;
            }

            @Override
            public Tuple2<String, Long> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                String message = new String(record.value(), StandardCharsets.UTF_8);
                long timestamp = record.timestamp() / 1000;
                return Tuple2.of(message, timestamp);
            }
        };

        // 创建一个FlinkKafka的消费者，其中包含kafka中的value和该条消息到kafka的时间
        return new FlinkKafkaConsumer<>(topicName, deserializationSchema, props);
    }


    /**
     * 获取带有时间戳的Kafka消费者
     * 该方法用于创建一个Flink Kafka消费者，该消费者能够消费Kafka主题中的消息，并附加消息的时间戳
     *
     * @param topicName Kafka主题名，消费者将从该主题消费消息
     * @param groupId 消费者组ID，用于Kafka消费者属性配置
     * @param timestamp 指定消费者开始消费的时间戳，单位为毫秒
     * @return 返回一个FlinkKafkaConsumer对象，用于消费Kafka中的消息
     */
    public static FlinkKafkaConsumer<Tuple2<String, Long>> getKafkaConsumerAndTimestamp(String topicName, String groupId, Long timestamp) {

        // 获取kafka的配置对象
        Properties props = KafkaUtil.getKafkaConsumerProperties(groupId);

        // 自定义kafka的反序列化器
        KafkaDeserializationSchema<Tuple2<String, Long>> deserializationSchema = new KafkaDeserializationSchema<Tuple2<String, Long>>() {

            @Override
            public TypeInformation<Tuple2<String, Long>> getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                });
            }

            @Override
            public boolean isEndOfStream(Tuple2<String, Long> nextElement) {
                return false;
            }

            @Override
            public Tuple2<String, Long> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                String message = new String(record.value(), StandardCharsets.UTF_8);
                long timestamp = record.timestamp() / 1000;
                return Tuple2.of(message, timestamp);
            }
        };

        // 创建一个FlinkKafkaConsumer对象，包含kafka中的value和该条消息到kafka的时间
        FlinkKafkaConsumer<Tuple2<String, Long>> consumer = new FlinkKafkaConsumer<>(topicName, deserializationSchema, props);

        // 设置从指定时间戳开始消费
        logger.info(">>>>> 从kafka的指定时间戳开始消费，时间戳：" + timestamp);
        consumer.setStartFromTimestamp(timestamp);

        // 返回消费者对象
        return consumer;
    }


    /**
     * 获取Flink Kafka生产者
     * 该方法用于创建并配置一个Flink Kafka生产者实例，以便将数据写入Kafka主题
     *
     * @param topicName Kafka主题名，数据将被写入该主题
     * @return 返回配置好的Flink Kafka生产者实例
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topicName) {
        // 使用ModelUtil从配置中获取Kafka的bootstrap.servers配置，并指定主题名和序列化方案来创建Flink Kafka生产者
        return new FlinkKafkaProducer<>(ModelUtil.getConfigValue("bootstrap.servers"), topicName, new SimpleStringSchema());
    }


    /**
     * 获取用于精确一次语义的 Kafka 生产者
     * 该方法配置并返回一个 Flink Kafka Producer，确保每个记录都被处理且仅被处理一次
     *
     * @param serializationSchema 序列化方案，定义如何将记录序列化到 Kafka
     * @param <T> 泛型参数，表示记录的类型
     * @return 配置好的 Flink Kafka Producer 实例
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducerForExactlyOnce(KafkaSerializationSchema<T> serializationSchema) {

        // 获取 Kafka 生产者属性配置
        Properties props = KafkaUtil.getKafkaProducerProperties();

        // 返回配置好的 Flink Kafka Producer 实例，指定默认主题、序列化方案、生产者属性以及精确一次语义
        return new FlinkKafkaProducer<>(ModelUtil.getConfigValue("kafka.default.topic"), serializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

    }


    /**
     * 创建一个Flink Kafka生产者，用于精确一次语义（Exactly Once）的Kafka消息发送
     * 精确一次语义确保每个消息被发送并仅被消费一次，即使在故障情况下也能保证数据的准确性和一致性
     *
     * @param topicName Kafka主题名称，消息将被发送到这个主题
     * @param <T> 泛型参数，表示消息的有效载荷类型
     * @return 返回一个Flink Kafka生产者实例，配置为精确一次语义
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducerForExactlyOnce(String topicName) {
        // 使用KafkaUtil工具类中的方法获取精确一次语义配置的Flink Kafka生产者
        // 序列化方案将泛型对象t转换为JSON字节流，并发送到指定的主题
        return KafkaUtil.getKafkaProducerForExactlyOnce((KafkaSerializationSchema<T>) (t, aLong) -> new ProducerRecord<>(topicName, JSON.toJSONBytes(t)));
    }


    /**
     * 获取一个确保数据恰好被处理一次的Flink Kafka生产者，使用Avro格式进行序列化
     *
     * @param topicName Kafka主题名称，生产者将向该主题发送消息
     * @param clz Avro记录的类类型，用于反序列化
     * @return 返回一个Flink Kafka生产者实例，配置为恰好一次处理语义
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducerAvroForExactlyOnce(String topicName, Class<T> clz) {
        return KafkaUtil.getKafkaProducerForExactlyOnce((KafkaSerializationSchema<T>) (element, timestamp) -> {
            // 创建一个字节数组输出流，用于存储序列化后的数据
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            // 获取一个二进制编码器，用于将数据编码为Avro格式
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            // 创建一个Avro数据写入器，用于将Java对象写入为Avro格式
            ReflectDatumWriter<T> writer = new ReflectDatumWriter<>(ReflectData.AllowNull.get().getSchema(clz));
            byte[] value = null;
            try {
                // 使用写入器将元素序列化为Avro格式并写入到编码器中
                writer.write(element, encoder);
                // 刷新编码器，确保所有数据都被写入到输出流中
                encoder.flush();
                // 将输出流转换为字节数组，即Avro序列化后的数据
                value = out.toByteArray();
                // 关闭输出流，释放资源
                out.close();
            } catch (IOException e) {
                // 如果序列化过程中发生异常，抛出运行时异常
                throw new RuntimeException("将数据序列化成Avro格式异常，异常信息如下 \r\n " + e.getMessage());
            }
            // 返回一个生产者记录，包含主题名称和序列化后的数据
            return new ProducerRecord<>(topicName, value);
        });
    }



}
