package com.admin.flink.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author sungaohua
 */
public class Flink06_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        // 处理数据   统计各省份 各广告的点击次数
        SingleOutputStreamOperator<Tuple2<String, Long>> map = streamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");

                return Tuple2.of(split[1] + "-" + split[2], 1L);
            }
        });

        KeyedStream<Tuple2<String, Long>, Tuple> tuple2TupleKeyedStream = map.keyBy(0);

        tuple2TupleKeyedStream.sum(1).print();

        env.execute();
    }
}
