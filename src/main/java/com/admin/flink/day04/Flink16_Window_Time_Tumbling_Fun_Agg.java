package com.admin.flink.day04;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink16_Window_Time_Tumbling_Fun_Agg {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.对数据进行处理，封装成Tuple元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                    out.collect(Tuple2.of(value, 1L));

            }
        });

        //4.将相同元素的数据聚和到一块
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneDStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        // 5.开启基于时间滚动窗口 ->窗口大小为5S
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //TODO 6.使用窗口增量聚和函数，显示单词累加的功能
        SingleOutputStreamOperator<Long> aggregate = window.aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
            @Override
            public Long createAccumulator() {
                System.out.println("初始化累加器。。");
                return 0L;
            }

            @Override
            public Long add(Tuple2<String, Long> value, Long accumulator) {
                System.out.println("累加操作。。。");
                return accumulator += value.f1;
            }

            @Override
            public Long getResult(Long accumulator) {
                System.out.println("输出结果。。。。");
                return accumulator;
            }

            @Override
            public Long merge(Long a, Long b) {
                return null;
            }
        });

        aggregate.print();

        env.execute();
    }
}
