package com.johnny.flink.basic.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 11:09
 */
public class TransformationDemo4 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Long> longDs = env.fromSequence(0, 100);

        SingleOutputStreamOperator<Long> filterDs = longDs.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long num) throws Exception {
                return num > 10;
            }
        });

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result1 = filterDs.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                // 获取分区编号/子任务编号
                int id = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(id, 1);
            }
        }).keyBy(t -> t.f0).sum(1);


        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result2 = filterDs.rebalance()
                .map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
                         @Override
                         public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                             //获取分区编号/子任务编号
                             int id = getRuntimeContext().getIndexOfThisSubtask();
                             return Tuple2.of(id, 1);
                         }
                     }
                ).keyBy(t -> t.f0).sum(1);

        //4.sink
        //result1.print();//有可能出现数据倾斜
        result2.print();//在输出前进行了rebalance重分区平衡,解决了数据倾斜

        //5.execute
        env.execute();

    }

}
