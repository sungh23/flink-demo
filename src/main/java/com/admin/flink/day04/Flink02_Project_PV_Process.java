package com.admin.flink.day04;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author sungaohua
 */
public class Flink02_Project_PV_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //实现方式二 Process
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<Tuple2<String, Long>> pv = streamSource.process(new ProcessFunction<String, Tuple2<String, Long>>() {

            Long count = 0L;

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                if ("pv".equals(split[3])) {
                    count++;
                }
                out.collect(Tuple2.of("pv", count));
            }
        });

        pv.print();

        env.execute();

    }
}
