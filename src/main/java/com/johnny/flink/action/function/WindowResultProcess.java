package com.johnny.flink.action.function;

import com.johnny.flink.action.model.CategoryPojo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/14 17:51
 */
public class WindowResultProcess extends ProcessWindowFunction<CategoryPojo, Object, String, TimeWindow> {
    @Override
    public void process(String dateTime, ProcessWindowFunction<CategoryPojo, Object, String, TimeWindow>.Context context, Iterable<CategoryPojo> iterable, Collector<Object> collector) throws Exception {

        //Java中的大小顶堆可以使用优先级队列来实现
        //https://blog.csdn.net/hefenglian/article/details/81807527
        //注意:
        // 小顶堆用来计算:最大的topN
        // 大顶堆用来计算:最小的topN
        // 初始容量，正常排序，就是小的的在前面大的在后面，c1>c2返回1 小顶堆
        PriorityQueue<CategoryPojo> queue = new PriorityQueue<>(3, (c1, c2) ->
                c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1);

        double totalPrice = 0D;
        double roundPrice = 0D;
        Iterator<CategoryPojo> iterator = iterable.iterator();
        for (CategoryPojo categoryPojo : iterable) {
            double price = categoryPojo.getTotalPrice();
            totalPrice += price;
            BigDecimal bigDecimal = new BigDecimal(price);
            roundPrice = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            // * -2.计算出各个分类的销售额top3,其实就是对各个分类的price进行排序取前3
            //注意:我们只需要top3,也就是只关注最大的前3个的顺序,剩下不管!所以不要使用全局排序,只需要做最大的前3的局部排序即可
            //那么可以使用小顶堆,把小的放顶上
            // c:80
            // b:90
            // a:100
            //那么来了一个数,和最顶上的比,如d,
            //if(d>顶上),把顶上的去掉,把d放上去,再和b,a比较并排序,保证顶上是最小的
            //if(d<=顶上),不用变
            if (queue.size() < 3) {
                queue.add(categoryPojo);
            } else {
                CategoryPojo top = queue.peek();
                if (categoryPojo.getTotalPrice() > top.getTotalPrice()) {
                    queue.poll();
                    queue.add(categoryPojo);
                }
            }
            List<String> top3Result = queue.stream()
                    .sorted((c1, c2) -> c1.getTotalPrice() > c2.getTotalPrice() ? -1 : 1)//逆序
                    .map(c -> "(分类：" + c.getCategory() + " 销售总额：" + c.getTotalPrice() + ")")
                    .collect(Collectors.toList());
            System.out.println("时间 ： " + dateTime + "  总价 : " + roundPrice + " top3:\n" + StringUtils.join(top3Result, ",\n"));
            System.out.println("-------------");

        }
    }
}
