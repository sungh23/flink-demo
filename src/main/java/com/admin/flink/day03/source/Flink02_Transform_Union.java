package com.admin.flink.day03.source;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

/**
 * @author sungaohua
 */
public class Flink02_Transform_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取两条数据流
        DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        DataStreamSource<Integer> source2 = env.fromCollection(Arrays.asList(2, 3, 1));
        DataStreamSource<Integer> source3 = env.fromCollection(Arrays.asList(2, 3, 1));

        //union 将两条（或者多条）流首尾连接在一起  要求流内的数据类型一致
        DataStream<Integer> union = source.union(source2);
        DataStream<Integer> union1 = union.union(source2);


        union1.print();


        env.execute();
    }
}
