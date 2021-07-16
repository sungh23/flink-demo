package com.admin.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * 批数据处理
 * @author sungaohua
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //1、 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //2、读取数据
        DataSource<String> source = env.readTextFile("input/word.txt");

        // 执行WordCount  flatMap
        FlatMapOperator<String, String> flatDStream = source.flatMap(new MyFlatMapFun());

        // 转换格式
        MapOperator<String, Tuple2<String, Long>> mapDStream = flatDStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        });

        UnsortedGrouping<Tuple2<String, Long>> groupBy = mapDStream.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> sum = groupBy.sum(1);

        sum.print();


    }

    public static class MyFlatMapFun implements FlatMapFunction<String,String>{
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] strings = value.split(" ");
            for (String word : strings) {
                out.collect(word);
            }
        }
    }
}
