package com.johnny.flink.advance.source;

import com.johnny.flink.advance.model.Goods;
import com.johnny.flink.advance.model.OrderItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;
import java.util.UUID;
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
 * @date 2025/3/17 20:28
 */
public class OrderItemSource extends RichSourceFunction<OrderItem> {

    private Boolean isCancel;
    private Random r;

    @Override
    public void open(Configuration parameters) throws Exception {
        isCancel = false;
        r = new Random();
    }

    @Override
    public void run(SourceContext<OrderItem> sourceContext) throws Exception {
        while(!isCancel) {
            Goods goods = Goods.randomGoods();
            OrderItem orderItem = new OrderItem();
            orderItem.setGoodsId(goods.getGoodsId());
            orderItem.setCount(r.nextInt(10) + 1);
            orderItem.setItemId(UUID.randomUUID().toString());
            sourceContext.collect(orderItem);
            orderItem.setGoodsId("111");
            sourceContext.collect(orderItem);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isCancel = true;
    }
}
