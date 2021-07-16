package com.admin.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  有界流数据处理
 * @author sungaohua
 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        // 获取流处理执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        // 读取数据
        DataStreamSource<String> source = senv.readTextFile("input/word.txt");
        // 处理数据
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = source.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] str = value.split(" ");
                for (String word : str) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        // 计算
        KeyedStream<Tuple2<String, Long>, Tuple> keyBy = flatMap.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyBy.sum(1);

        sum.print();

        senv.execute();

    }
}
