package com.admin.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  无界流数据处理
 * @author sungaohua
 */
public class Flink03_WordCount_UnBounded {
    public static void main(String[] args) throws Exception {
        // 获取流处理环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        DataStreamSource<String> source = senv.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = source.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] strings = value.split(" ");
                for (String str : strings) {
                    out.collect(Tuple2.of(str, 1L));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, String> keyBy = flatMap.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyBy.sum(1);

        sum.print();

        senv.execute();
    }
}
