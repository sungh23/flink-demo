package com.admin.flink.day07;

import com.admin.flink.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.elasticsearch.persistent.decider.AssignmentDecision;

import javax.lang.model.element.VariableElement;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author sungaohua
 */
public class Flink13_CEP_Loggin_Fail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件中读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/LoginLog.csv");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<LoginEvent> streamOperator = streamSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new LoginEvent(Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                }));

        KeyedStream<LoginEvent, Long> keyBy = streamOperator.keyBy(r -> r.getUserId());

        //定义模式  连续两秒失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                    //两次及以上  严格连续  超时两秒的不考虑
                }).timesOrMore(2).consecutive().within(Time.seconds(2));

        //将流应用到模式上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyBy, pattern);

        //将符合规则的数据输出到控制台上
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();


        env.execute();
    }
}
