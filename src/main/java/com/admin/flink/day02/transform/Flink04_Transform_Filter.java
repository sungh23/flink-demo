package com.admin.flink.day02.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author sungaohua
 */
public class Flink04_Transform_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        // 读取数据
//        DataStreamSource<String> streamSource = senv.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> streamSource = senv.readTextFile("input");

        streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {

                return value.equals("hello atguigu");
            }
        }).print();

        senv.execute();
    }
}
