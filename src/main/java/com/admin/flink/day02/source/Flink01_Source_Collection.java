package com.admin.flink.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author sungaohua
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        //创建集合
        List<String> list = Arrays.asList("1", "2", "3", "4");

        //从集合中获取数据
        DataStreamSource<String> fromCollection = senv.fromCollection(list);

        fromCollection.print();

        senv.execute();

    }
}
