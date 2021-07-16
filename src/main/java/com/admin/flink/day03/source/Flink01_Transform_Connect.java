package com.admin.flink.day03.source;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

/**
 * @author sungaohua
 */
public class Flink01_Transform_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取两条数据流
        DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        DataStreamSource<String> source2 = env.fromCollection(Arrays.asList("1", "2", "3", "4"));

        // connect 算子 只是简单的将两条数据流并在了一起 并没有对其做什么操作，流内的数据格式也不会有变化
        ConnectedStreams<Integer, String> connect = source.connect(source2);

        SingleOutputStreamOperator<Object> map = connect.map(new CoMapFunction<Integer, String, Object>() {
            @Override
            public Object map1(Integer value) throws Exception {
                return value + 1;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value + "map2";
            }
        });

        map.print();


        env.execute();
    }
}
