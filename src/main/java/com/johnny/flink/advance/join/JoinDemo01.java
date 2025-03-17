package com.johnny.flink.advance.join;

import com.johnny.flink.advance.model.FactOrderItem;
import com.johnny.flink.advance.model.Goods;
import com.johnny.flink.advance.model.OrderItem;
import com.johnny.flink.advance.source.GoodsSource;
import com.johnny.flink.advance.source.OrderItemSource;
import lombok.Data;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
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
 *需求：
 * 使用两个指定Source模拟数据，一个Source是订单明细，一个Source是商品数据。
 * 我们通过window join，将数据关联到一起。
 *
 * 思路：
 *
 * 1、Window Join首先需要使用where和equalTo指定使用哪个key来进行关联，此处我们通过应用方法，基于GoodsId来关联两个流中的元素。
 *
 * 2、设置5秒的滚动窗口，流的元素关联都会在这个5秒的窗口中进行关联。
 *
 * 3、apply方法中实现将两个不同类型的元素关联并生成一个新类型的元素。
 *
 * @author wan.liang(79274)
 * @date 2025/3/17 20:00
 */
public class JoinDemo01 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Goods> goodsDs = env.addSource(new GoodsSource(), TypeInformation.of(Goods.class)).assignTimestampsAndWatermarks(new GoodsWaterMark());
        SingleOutputStreamOperator<OrderItem> orderItemDs = env.addSource(new OrderItemSource(), TypeInformation.of(OrderItem.class)).assignTimestampsAndWatermarks(new OrderItemWatermark());

        // 关联查询
        DataStream<FactOrderItem> joinDs = orderItemDs.join(goodsDs)
                // 第一个流
                .where(OrderItem::getGoodsId)
                // 第二流
                .equalTo(Goods::getGoodsId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((item, goods) -> {
                    FactOrderItem factOrderItem = new FactOrderItem();
                    factOrderItem.setGoodsId(goods.getGoodsId());
                    factOrderItem.setGoodsName(goods.getGoodsName());
                    factOrderItem.setCount(new BigDecimal(item.getCount()));
                    factOrderItem.setTotalMoney(goods.getGoodsPrice().multiply(new BigDecimal(item.getCount())));
                    return factOrderItem;
                });

        joinDs.print();

        env.execute();
    }


    /**
     * 构建水印分配器，直接用系统时间
     */
    public static class GoodsWaterMark implements WatermarkStrategy<Goods> {
        @Override
        public WatermarkGenerator<Goods> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<Goods>() {
                @Override
                public void onEvent(Goods event, long eventTimestamp, WatermarkOutput output) {
                    output.emitWatermark(new Watermark(System.currentTimeMillis()));
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    output.emitWatermark(new Watermark(System.currentTimeMillis()));
                }
            };
        }

        @Override
        public TimestampAssigner<Goods> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element, recordTimestamp) -> System.currentTimeMillis();
        }
    }
    public static class OrderItemWatermark implements WatermarkStrategy<OrderItem> {
            @Override
            public TimestampAssigner<OrderItem> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return (element, recordTimestamp) -> System.currentTimeMillis();
            }
            @Override
            public WatermarkGenerator<OrderItem> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<OrderItem>() {
                    @Override
                    public void onEvent(OrderItem event, long eventTimestamp, WatermarkOutput output) {
                        output.emitWatermark(new Watermark(System.currentTimeMillis()));
                    }
                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(System.currentTimeMillis()));
                    }
                };
            }
    }


}
