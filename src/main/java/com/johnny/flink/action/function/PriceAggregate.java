package com.johnny.flink.action.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/14 17:30
 */
public class PriceAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
    @Override
    public Double createAccumulator() {
        return 0D;
    }

    @Override
    public Double add(Tuple2<String, Double> value, Double accumulator) {
        return value.f1 + accumulator;
    }

    @Override
    public Double getResult(Double accumulator) {
        return accumulator;
    }

    @Override
    public Double merge(Double a, Double b) {
        return a + b;
    }
}
