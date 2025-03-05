package com.johnny.flink.basic.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <b>基于文件的Source</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 10:28
 */
public class SourceFile {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> ds1 = env.readTextFile("D:\\Project\\IDEA\\bigdata-study-tutorial\\flink-tutorial-java\\src\\main\\data\\input\\words.txt");
        DataStream<String> ds2 = env.readTextFile("data/input/dir");
        DataStream<String> ds3 = env.readTextFile("hdfs://node1:8020//wordcount/input/words.txt");
        DataStream<String> ds4 = env.readTextFile("data/input/wordcount.txt.gz");

        env.execute();
    }

}
