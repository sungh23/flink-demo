package com.admin.flink.day04;

import com.admin.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author sungaohua
 */
public class Flink01_Project_PV {
    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //实现方式一 WordCount
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        // 转换为样例类
        SingleOutputStreamOperator<UserBehavior> map = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4]));
            }
        });

        // 过滤出pv数据
        SingleOutputStreamOperator<UserBehavior> filter = map.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        // 转换为tuple类型数据
        SingleOutputStreamOperator<Tuple2<String, Long>> toOneTuple = filter.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of(value.getBehavior(), 1L);
            }
        });

        // 聚合
        KeyedStream<Tuple2<String, Long>, String> keyedStream = toOneTuple.keyBy(r -> r.f0);

        keyedStream.sum(1).print();

        long end = System.currentTimeMillis();

        System.out.println("运行时间："+(end-start));


        env.execute();
    }
}
