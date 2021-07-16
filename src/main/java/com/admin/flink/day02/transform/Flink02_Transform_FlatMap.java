package com.admin.flink.day02.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author sungaohua
 */
public class Flink02_Transform_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        // 读取数据
        DataStreamSource<String> streamSource = senv.socketTextStream("hadoop102", 9999);

        /*
        flatMap 消费一个元素 并生成零个或多个元素
         */
        streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).print();

        senv.execute();
    }
}
