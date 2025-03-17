package com.johnny.flink.action.source;

import com.johnny.flink.action.model.CategoryPojo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/17 15:48
 */
public class OrderSource implements SourceFunction<Tuple3<String, String, Long>> {

    private boolean flag = true;

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> sourceContext) throws Exception {
        Random random = new Random();
        while (flag) {
            String userId = random.nextInt(5) + "";
            String orderId = UUID.randomUUID().toString();
            long currentTimeMillis = System.currentTimeMillis();
            sourceContext.collect(Tuple3.of(userId, orderId, currentTimeMillis));
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
