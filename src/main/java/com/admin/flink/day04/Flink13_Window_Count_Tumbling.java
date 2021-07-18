package com.admin.flink.day04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class Flink13_Window_Count_Tumbling {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.对数据进行处理，封装成Tuple元组
        SingleOutputStreamOperator<String> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                out.collect(value);
            }

        });

        //4.将相同元素的数据聚和到一块
        KeyedStream<String, String> keyedStream = wordToOneDStream.keyBy(r -> r);

        //TODO 5.创建基于元素个数的滚动窗口
        WindowedStream<String, String, GlobalWindow> countWindow = keyedStream.countWindow(3);

        countWindow.process(new ProcessWindowFunction<String, String, String, GlobalWindow>() {
            @Override
            public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                for (String element : elements) {
                    out.collect(element);
                }
            }
        }).print();

        env.execute();
    }
}
