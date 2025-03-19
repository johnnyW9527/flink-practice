package com.johnny.flink.advance.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.johnny.flink.utils.KafkaUtil;
import com.johnny.flink.utils.ModelUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/18 19:37
 */
public class GmallCDC {

    private static Logger logger = LoggerFactory.getLogger(GmallCDC.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ModelUtil.deployRocksdbCheckpoint(env, "GmallCDC", 1000 * 60 * 5);

        // 通过cdc构建source
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(ModelUtil.getConfigValue("mysql.host"))
                .port(Integer.parseInt(ModelUtil.getConfigValue("mysql.port")))
                .username(ModelUtil.getConfigValue("mysql.username"))
                .password(ModelUtil.getConfigValue("mysql.password"))
                .databaseList(ModelUtil.getConfigValue("mysql.database"))
                .deserializer(new CustomDeserializer())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> streamDs = env.addSource(sourceFunction);

        streamDs.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).addSink(KafkaUtil.getKafkaProducerForExactlyOnce("test"));

        env.execute();
    }


}
