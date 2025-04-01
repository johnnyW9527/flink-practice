package com.johnny.flink.webproject.job;

import com.johnny.flink.webproject.model.LoginEvent;
import com.johnny.flink.webproject.model.LoginFailWarning;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/4/1 10:10
 */
public class LoginFailWithCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        URL resource = LoginFailWithCep.class.getResource("/temp/LoginLog.csv");
        SingleOutputStreamOperator<LoginEvent> inputDs = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L));

        // 定义一个匹配模式
        // firstFail -> secondFail  within 2s
        Pattern<LoginEvent, LoginEvent> patternDs = Pattern.<LoginEvent>begin("firstFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                }).next("secondFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                })
                .next("thirdFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                }).within(Time.seconds(3));


        Pattern<LoginEvent, LoginEvent> failEventsPattern = Pattern.<LoginEvent>begin("failEvents")
                .where(new SimpleCondition<LoginEvent>() {

                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                }).times(3).consecutive().within(Time.seconds(5));

        PatternStream<LoginEvent> resultDs
                = CEP.pattern(inputDs.keyBy(LoginEvent::getUserId), patternDs);

        SingleOutputStreamOperator<LoginFailWarning> selectDs = resultDs.select(new LoginFailMatchDetectWarning());
        selectDs.print();


        env.execute();
    }

    public static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent, LoginFailWarning> {

        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> map) throws Exception {
            LoginEvent firstFail = map.get("firstFail").get(0);
            LoginEvent thirdFail = map.get("thirdFail").get(0);
            return new LoginFailWarning(firstFail.getUserId(), firstFail.getTimestamp(), thirdFail.getTimestamp(), "login fail in 2s for 3 times");
        }
    }

}
