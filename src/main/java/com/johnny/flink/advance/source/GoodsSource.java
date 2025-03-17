package com.johnny.flink.advance.source;

import com.johnny.flink.advance.model.Goods;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/17 20:29
 */
public class GoodsSource extends RichSourceFunction<Goods> {

    private Boolean isCancel;

    @Override
    public void open(Configuration parameters) throws Exception {
        isCancel = Boolean.FALSE;
    }

    @Override
    public void run(SourceContext<Goods> sourceContext) throws Exception {
        while (!isCancel) {
            Goods.GOODS_LIST.forEach(sourceContext::collect);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isCancel = Boolean.TRUE;
    }
}
