package com.johnny.flink.advance.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/19 15:44
 */
public class CdcDwdDeserializationSchema implements DebeziumDeserializationSchema<JSONObject> {

    public static final long serialVersionUID = 1L;

    public CdcDwdDeserializationSchema() {
    }

    @Override
    public void deserialize(SourceRecord record, Collector<JSONObject> collector) throws Exception {
        Struct dataStruct = (Struct) record.value();

        Struct afterStruct = dataStruct.getStruct("after");
        Struct beforeStruct = dataStruct.getStruct("before");

        // 1.同时存在 beforeStruct 跟 afterStruct数据的话，就代表是update的数据
        // 2.只存在 beforeStruct 就是delete数据
        // 3.只存在 afterStruct 就是insert数据
        JSONObject logJson =  new JSONObject();
        String canal_type = "";
        List<Field> fieldList = null;
        if (afterStruct != null && beforeStruct != null) {
            canal_type = "update";
            fieldList = afterStruct.schema().fields();
            for (Field field : fieldList) {
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(fieldName);
                logJson.put(fieldName, fieldValue);
            }
        } else if (afterStruct != null) {
            canal_type = "insert";
            fieldList = afterStruct.schema().fields();
            for (Field field : fieldList) {
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(fieldName);
                logJson.put(fieldName, fieldValue);
            }
        } else if (beforeStruct != null) {
            canal_type = "delete";
            fieldList = beforeStruct.schema().fields();
            for (Field field : fieldList) {
                String fieldName = field.name();
                Object fieldValue = beforeStruct.get(fieldName);
                logJson.put(fieldName, fieldValue);
            }
        } else {
            canal_type = "unknown";
        }
        // database table 信息
        Struct source = dataStruct.getStruct("source");
        Object db = source.get("db");
        Object table = source.get("table");
        Object tsMs = source.get("ts_ms");
        logJson.put("canal_database", db);
        logJson.put("canal_table", table);
        logJson.put("canal_ts", tsMs);
        logJson.put("canal_type", canal_type);

        String topic = record.topic();
        // 主键字段
        Struct pk = (Struct) record.key();
        List<Field> pkFieldList = pk.schema().fields();
        int partitionerNum = 0;
        for (Field field : pkFieldList) {
            Object pkValue = pk.get(field.name());
            partitionerNum += pkValue.hashCode();
        }
        int hash = Math.abs(partitionerNum) % 3;
        logJson.put("pk_hashcode", hash);
        collector.collect(logJson);
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return BasicTypeInfo.of(JSONObject.class);
    }
}
