package com.johnny.flink.action.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/14 17:21
 */
public class MySource implements SourceFunction<Tuple2<String, Double>> {

    private boolean flag = true;
    private String[] categorys = {"女装", "男装","图书", "家电","洗护", "美妆","运动", "游戏","户外", "家具","乐器", "办公"};
    private Random random = new Random();

    @Override
    public void run(SourceContext<Tuple2<String, Double>> sourceContext) throws Exception {
        while (flag) {
            int index = random.nextInt(categorys.length);
            String category = categorys[index];
            double price = random.nextDouble() * 100;
            sourceContext.collect(Tuple2.of(category, price));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
