package com.admin.flink.day04;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;

/**
 * @author sungaohua
 */
public class Flink03_Project_UV_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //实现方式二 Process
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        HashSet<String> hashSet = new HashSet<>();
        SingleOutputStreamOperator<Tuple2<String, Long>> uv = streamSource.process(new ProcessFunction<String, Tuple2<String, Long>>() {

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                if ("pv".equals(split[3])){
                    hashSet.add(split[0]);
                }
                out.collect(Tuple2.of("UV", (long) hashSet.size()));
            }

        });

        uv.print();

        env.execute();

    }
}
