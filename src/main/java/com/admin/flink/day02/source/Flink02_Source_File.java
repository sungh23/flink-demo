package com.admin.flink.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author sungaohua
 */
public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);


        //从文件中获取数据 逐行读取
        DataStreamSource<String> fromCollection = senv.readTextFile("input");

        fromCollection.print();

        senv.execute();

    }
}
