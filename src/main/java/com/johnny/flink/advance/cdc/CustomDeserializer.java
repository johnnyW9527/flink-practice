package com.johnny.flink.advance.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * <b>自定义flinkcdc的反序列化器 </b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/19 11:15
 */
public class CustomDeserializer implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        /*
                 封装的数据格式
                 {
                     "database":"",
                     "tableName":"",
                     "before":{"id":"","tm_name":""....},
                     "after":{"id":"","tm_name":""....},
                     "type":"c u d",
                     "ts":156456135615
                 }
         */

        /*
                SourceRecord{
                    sourcePartition={server=mysql_binlog_source},
                    sourceOffset={ts_sec=1642091776, file=mysql-bin.000001, pos=4008355, row=1, server_id=1, event=2}
                }
                ConnectRecord{
                    topic='mysql_binlog_source.gmall.base_trademark', kafkaPartition=null, key=Struct{id=15},
                    keySchema=Schema{mysql_binlog_source.gmall.base_trademark.Key:STRUCT},
                    value=Struct{
                        before=Struct{id=15,tm_name=111},
                        after=Struct{id=15,tm_name=111,logo_url=11111111111},
                        source=Struct{
                            version=1.4.1.Final,
                            connector=mysql,
                            name=mysql_binlog_source,
                            ts_ms=1642091776000,
                            db=gmall,
                            table=base_trademark,
                            server_id=1,
                            file=mysql-bin.000001,
                            pos=4008492,
                            row=0,
                            thread=22
                        },
                        op=u,
                        ts_ms=1642091776679
                    },
                    valueSchema=Schema{
                        mysql_binlog_source.gmall.base_trademark.Envelope:STRUCT
                    },
                    timestamp=null,
                    headers=ConnectHeaders(headers=)
                }
         */

        //1.创建JSON对象用于存储最终数据
        JSONObject result = new JSONObject();
        //2.获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];
        //3.获取 "before"数据 和 "after"数据
        Struct value = (Struct) sourceRecord.value();
        // 3.1. "before"数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema beforeSchema = before.schema();
            for (Field field : beforeSchema.fields()) {
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }
        // 3.2. "after"数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema afterSchema = after.schema();
            for (Field field : afterSchema.fields()) {
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }
        //4.获取timestamp
        long ts = (long) value.get("ts_ms");
        //5.获取操作类型  CREATE UPDATE DELETE，并转换为小写，其中create转换为insert，方便后续写入
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }
        //6.将字段写入JSON对象
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);
        result.put("ts", ts);
        //7.输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
