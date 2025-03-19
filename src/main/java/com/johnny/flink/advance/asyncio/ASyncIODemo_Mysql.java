package com.johnny.flink.advance.asyncio;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
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
 * @date 2025/3/18 10:44
 */
public class ASyncIODemo_Mysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<CategoryInfo> sourceDs = env.addSource(new RichSourceFunction<CategoryInfo>() {

            private Boolean flag = true;

            @Override
            public void run(SourceContext<CategoryInfo> sourceContext) throws Exception {
                Integer[] ids = {1, 2, 3, 4, 5};
                for (Integer id : ids) {
                    sourceContext.collect(new CategoryInfo(id, null));
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        // 方式一 java-vertx中提示的异步client实现异步IO
        // unorderedWait 无序等待
        SingleOutputStreamOperator<CategoryInfo> result1Ds = AsyncDataStream.orderedWait(sourceDs, new AsyncIOFunction1(), 1000, TimeUnit.SECONDS, 10);

        // 方式二 Mysql中同步client+线程池模拟异步IO
        // unorderedWait 无序等待
        SingleOutputStreamOperator<CategoryInfo> result2Ds = AsyncDataStream.unorderedWait(sourceDs, new ASyncIOFunction2(), 1000, TimeUnit.SECONDS, 10);

        result1Ds.print("方式一：Java-vertx中提供的异步client实现异步IO \n");
//        result2Ds.print("方式二：MySQL中同步client+线程池模拟异步IO \n");


        env.execute();
    }



    
}
