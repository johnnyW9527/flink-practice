package com.johnny.flink.basic.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * <b>基于集合的Source</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 10:21
 */
public class SourceCollection {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.fromElements(可变参数);
        //env.fromColletion(各种集合);
        //env.generateSequence(开始,结束);
        //env.fromSequence(开始,结束);
        DataStreamSource<String> ds1 = env.fromElements("hadoop", "spark", "flink");
        DataStreamSource<List<String>> ds2 = env.fromElements(Arrays.asList("hadoop", "spark", "flink"));
        // DataStreamSource<Long> ds3 = env.generateSequence(1, 10);

        ds1.print("ds1");
        ds2.print("ds2");

        env.execute();

    }

}
