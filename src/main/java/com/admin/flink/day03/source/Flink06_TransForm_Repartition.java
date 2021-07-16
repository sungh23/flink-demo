package com.admin.flink.day03.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author sungaohua
 */
public class Flink06_TransForm_Repartition {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).setParallelism(2);

        KeyedStream<String, String> keyedStream = map.keyBy(r -> r);

        DataStream<String> shuffle = map.shuffle();

        DataStream<String> rebalance = map.rebalance();

        DataStream<String> rescale = map.rescale();

        map.print("原始数据:").setParallelism(2);
        keyedStream.print("KeyBy:");
        shuffle.print("Shuffle:");
        rebalance.print("Rebalance:");
        rescale.print("Rescale:");

        env.execute();
    }
}
