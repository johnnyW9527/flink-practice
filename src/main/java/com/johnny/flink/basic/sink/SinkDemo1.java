package com.johnny.flink.basic.sink;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述
 *  * 1.ds.print 直接输出到控制台
 *  * 2.ds.printToErr() 直接输出到控制台,用红色
 *  * 3.ds.collect 将分布式数据收集为本地集合
 *  * 4.ds.setParallelism(1).writeAsText("本地/HDFS的path",WriteMode.OVERWRITE)<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 11:19
 */
public class SinkDemo1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> ds = env.readTextFile("data/input/words.txt");

        // 直接输出到控制台
        ds.print();
        // 直接输出到控制台,用红色
        ds.printToErr();
        ds.writeAsText("data/output/test", FileSystem.WriteMode.OVERWRITE).setParallelism(2);
        //注意:
        //Parallelism=1为文件
        //Parallelism>1为文件夹

        //5.execute
        env.execute();
    }

}
