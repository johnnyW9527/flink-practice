package com.johnny.flink.advance.asyncio;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/18 11:14
 */
public class AsyncIODemo_Redis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.readTextFile("data/input/city.txt");

        SingleOutputStreamOperator<String> result1 = AsyncDataStream.orderedWait(lines, new AsyncRedis(), 10, TimeUnit.SECONDS, 1);
        SingleOutputStreamOperator<String> result2 = AsyncDataStream.orderedWait(lines, new AsyncRedisByVertx(), 10, TimeUnit.SECONDS, 1);

        result1.print().setParallelism(1);
        result2.print().setParallelism(1);

        env.execute();



        env.execute();
    }

}
