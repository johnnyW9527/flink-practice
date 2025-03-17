package com.johnny.flink.advance;

import com.johnny.flink.action.source.MySource;
import lombok.SneakyThrows;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 * 事件流和配置流(需要广播为State)的关联,并实现配置的动态更新!
 * @author wan.liang(79274)
 * @date 2025/3/17 19:16
 */
public class BroadcastStateConfigUpdate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple4<String, String, String, Integer>> eventDs = env.addSource(new MySource());

        DataStreamSource<Map<String, Tuple2<String, Integer>>> configDs = env.addSource(new MySqlSource());

        MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> descriptor =
                new MapStateDescriptor<>("config-state", Types.VOID, Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));
        BroadcastStream<Map<String, Tuple2<String, Integer>>> broadcastDs
                = configDs.broadcast(descriptor);

        BroadcastConnectedStream<Tuple4<String, String, String, Integer>, Map<String, Tuple2<String, Integer>>> connectDs = eventDs.connect(broadcastDs);

        SingleOutputStreamOperator<Tuple6<String, String, String, Integer, String, Integer>> resultDs = connectDs.process(new BroadcastProcessFunction<Tuple4<String, String, String, Integer>, Map<String, Tuple2<String, Integer>>, Tuple6<String, String, String, Integer, String, Integer>>() {

            @Override
            public void processElement(Tuple4<String, String, String, Integer> value, BroadcastProcessFunction<Tuple4<String, String, String, Integer>, Map<String, Tuple2<String, Integer>>, Tuple6<String, String, String, Integer, String, Integer>>.ReadOnlyContext readOnlyContext, Collector<Tuple6<String, String, String, Integer, String, Integer>> collector) throws Exception {
                String userId = value.f0;
                ReadOnlyBroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = readOnlyContext.getBroadcastState(descriptor);
                if (broadcastState != null) {
                    Map<String, Tuple2<String, Integer>> map = broadcastState.get(null);
                    if (map != null) {
                        Tuple2<String, Integer> tuple2 = map.get(userId);

                        String userName = tuple2.f0;
                        Integer userAge = tuple2.f1;
                        collector.collect(new Tuple6<>(userId, value.f1, value.f2, value.f3, userName, userAge));

                    }
                }
            }

            @Override
            public void processBroadcastElement(Map<String, Tuple2<String, Integer>> stringTuple2Map, BroadcastProcessFunction<Tuple4<String, String, String, Integer>, Map<String, Tuple2<String, Integer>>, Tuple6<String, String, String, Integer, String, Integer>>.Context context, Collector<Tuple6<String, String, String, Integer, String, Integer>> collector) throws Exception {
                BroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = context.getBroadcastState(descriptor);
                broadcastState.clear();
                broadcastState.put(null, stringTuple2Map);
            }
        });

        resultDs.print();

        env.execute();


    }


    public static class MySource implements SourceFunction<Tuple4<String, String, String, Integer>> {

        private boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple4<String, String, String, Integer>> sourceContext) throws Exception {
            Random rnd = new Random();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            while (isRunning) {

                int id = rnd.nextInt(4) + 1;
                String user_id = "user_" + id;
                String eventTime = sdf.format(System.currentTimeMillis());
                String event_type = "type_" + rnd.nextInt(3);
                int pId = rnd.nextInt(4);
                sourceContext.collect(new Tuple4<>(user_id, eventTime, event_type, pId));
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class MySqlSource extends RichSourceFunction<Map<String, Tuple2<String, Integer>>> {

        private boolean isRunning = true;
        private Connection conn = null;
        private PreparedStatement stmt = null;
        private ResultSet rs = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://10.21.16.154:3306/dsg25_wanliang?serverTimezone=GMT%2B8&characterEncoding=utf8&useUnicode=true&useSSL=false",
                    "root", "asiainfo");
            stmt = conn.prepareStatement("select userID, userName, userAge from user_info");
        }

        @Override
        public void run(SourceContext<Map<String, Tuple2<String, Integer>>> sourceContext) throws Exception {
            while (isRunning) {
                Map<String, Tuple2<String, Integer>> map = new HashMap<>();
                rs = stmt.executeQuery();
                while (rs.next()) {
                    String userID = rs.getString("userID");
                    String userName = rs.getString("userName");
                    int userAge = rs.getInt("userAge");
                    map.put(userID,Tuple2.of(userName,userAge));
                }
                sourceContext.collect(map);
                Thread.sleep(5000);
            }
        }

        @SneakyThrows
        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void close() throws Exception {
            if (conn != null) {
                conn.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (rs != null) {
                rs.close();
            }
        }
    }

}
