package com.johnny.flink.basic.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 11:04
 */
public class TransformationDemo3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        OutputTag<Integer> tag_even = new OutputTag<Integer>("偶数", TypeInformation.of(Integer.class)){};
        OutputTag<Integer> tag_odd = new OutputTag<Integer>("奇数", TypeInformation.of(Integer.class)) {};

        SingleOutputStreamOperator<Integer> tagResult = ds.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer integer, ProcessFunction<Integer, Integer>.Context context, Collector<Integer> collector) throws Exception {
                if (integer % 2 == 0) {
                    context.output(tag_even, integer);
                } else {
                    context.output(tag_odd, integer);
                }
            }
        });

        //取出标记好的数据
        DataStream<Integer> evenResult = tagResult.getSideOutput(tag_even);
        DataStream<Integer> oddResult = tagResult.getSideOutput(tag_odd);

        //4.Sink
        evenResult.print("偶数");
        oddResult.print("奇数");

        //5.execute
        env.execute();
    }

}
