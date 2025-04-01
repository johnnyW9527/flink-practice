package com.johnny.flink.webproject.function;

import com.google.common.collect.Lists;
import com.johnny.flink.webproject.model.MarketingUserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Random;

/**
 * <b>模拟市场用户行为数据源</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/25 20:12
 */
public class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {

    Boolean running = true;

    // 定义用户行为和渠道的范围
    List<String> behaviors = Lists.newArrayList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
    List<String> channelList = Lists.newArrayList("app store", "wechat", "weibo");

    Random random = new Random();

    @Override
    public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
        while (running) {
            long id = random.nextLong();
            String behavior = behaviors.get(random.nextInt(behaviors.size()));
            String channel = channelList.get(random.nextInt(channelList.size()));
            long timestamp = System.currentTimeMillis();
            sourceContext.collect(new MarketingUserBehavior(id, behavior, channel, timestamp));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
