package com.johnny.flink.advance.join;

import com.johnny.flink.advance.model.FactOrderItem;
import com.johnny.flink.advance.model.Goods;
import com.johnny.flink.advance.model.OrderItem;
import com.johnny.flink.advance.source.GoodsSource;
import com.johnny.flink.advance.source.OrderItemSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *1、通过keyBy将两个流join到一起
 *
 * 2、interval join需要设置流A去关联哪个时间范围的流B中的元素。此处，我设置的下界为-1、上界为0，且上界是一个开区间。表达的意思就是流A中某个元素的时间，对应上一秒的流B中的元素。
 *
 * 3、process中将两个key一样的元素，关联在一起，并加载到一个新的FactOrderItem对象中
 * @author wan.liang(79274)
 * @date 2025/3/17 20:24
 */
public class JoinDemo02 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Goods> goodsDs = env.addSource(new GoodsSource(), TypeInformation.of(Goods.class)).assignTimestampsAndWatermarks(new JoinDemo01.GoodsWaterMark());
        SingleOutputStreamOperator<OrderItem> orderItemDs = env.addSource(new OrderItemSource(), TypeInformation.of(OrderItem.class)).assignTimestampsAndWatermarks(new JoinDemo01.OrderItemWatermark());

        SingleOutputStreamOperator<FactOrderItem> resultDs = orderItemDs.keyBy(OrderItem::getGoodsId)
                .intervalJoin(goodsDs.keyBy(Goods::getGoodsId))
                .between(Time.seconds(-1), Time.seconds(0))
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<OrderItem, Goods, FactOrderItem>() {
                    @Override
                    public void processElement(OrderItem orderItem, Goods goods, ProcessJoinFunction<OrderItem, Goods, FactOrderItem>.Context context, Collector<FactOrderItem> collector) throws Exception {
                        FactOrderItem factOrderItem = new FactOrderItem();
                        factOrderItem.setGoodsId(goods.getGoodsId());
                        factOrderItem.setGoodsName(goods.getGoodsName());
                        factOrderItem.setCount(new BigDecimal(orderItem.getCount()));
                        factOrderItem.setTotalMoney(goods.getGoodsPrice().multiply(new BigDecimal(orderItem.getCount())));

                        collector.collect(factOrderItem);
                    }
                });

        resultDs.print();


        env.execute();

    }

}
