package com.johnny.flink.action.function;


import com.johnny.flink.action.model.CategoryPojo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/14 17:31
 */
public class WindowResult implements WindowFunction<Double, CategoryPojo, String, TimeWindow> {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    @Override
    public void apply(String catagory, TimeWindow timeWindow, Iterable<Double> iterable, Collector<CategoryPojo> collector) throws Exception {

        Double price = iterable.iterator().next();

        BigDecimal bigDecimal = new BigDecimal(price);
        //setScale设置精度保留2位小数
        // 四舍五入
        double roundPrice = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();

        long currentTimeMillis = System.currentTimeMillis();
        String dataTime = sdf.format(currentTimeMillis);

        CategoryPojo categoryPojo = new CategoryPojo(catagory, roundPrice, dataTime);

        collector.collect(categoryPojo);
    }
}
