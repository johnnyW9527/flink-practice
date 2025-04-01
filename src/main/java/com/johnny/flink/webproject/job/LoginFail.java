package com.johnny.flink.webproject.job;

import com.google.common.collect.Lists;
import com.johnny.flink.webproject.model.LoginEvent;
import com.johnny.flink.webproject.model.LoginFailWarning;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.time.Duration;
import java.util.List;

/**
 * <b>恶意登陆监测</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/4/1 09:56
 */
public class LoginFail {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        URL resource = LoginFail.class.getResource("/temp/LoginLog.csv");
        SingleOutputStreamOperator<LoginEvent> loginEventDs = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L));


        // 自定义处理函数检测连续登陆失败事件
        SingleOutputStreamOperator<LoginFailWarning> resultDs = loginEventDs.keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));

        resultDs.print();

        env.execute();
    }

    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        // 定义属性，最大连续登录失败次数
        private final Integer maxFailTimes;

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        // 定义状态，保存2秒内所有登陆失败事件
        ListState<LoginEvent> loginFailEventListState;
        // 保存注册的定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(LoginEvent loginEvent, KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.Context context, Collector<LoginFailWarning> collector) throws Exception {
            // 判断当前登陆事件
            if (loginEvent.getLoginState().equals("fail")) {
                // 如果是失败事件，添加到列表状态
                loginFailEventListState.add(loginEvent);
                // 如果没有定时器，注册一个2秒之后的定时器
                if (timerTsState.value() == null) {
                    long ts = (loginEvent.getTimestamp() + 2) * 1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            } else {
                if (timerTsState.value() != null) {
                    context.timerService().deleteEventTimeTimer(timerTsState.value());
                }
                loginFailEventListState.clear();
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            // 定时器触发，说明2秒内没有登录成功，判断列表长度
            List<LoginEvent> loginFailList = Lists.newArrayList(loginFailEventListState.get());
            int failTimes = loginFailList.size();
            if (failTimes >= maxFailTimes) {
                // 如果超过最大失败次数，输出报警
                out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                        loginFailList.get(0).getTimestamp(),
                        loginFailList.get(failTimes - 1).getTimestamp(),
                        "login fail in 2s for " + failTimes + " times"));
            }
            loginFailEventListState.clear();
            timerTsState.clear();
        }
    }


}
