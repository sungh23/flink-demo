package com.admin.flink.day02.transform;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * @author sungaohua
 */
public class Flink03_Transform_RichFlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

//        DataStreamSource<String> streamSource = senv.socketTextStream("hadoop102", 9999);
//        DataStreamSource<String> streamSource = senv.readTextFile("input");
        List<String> list = Arrays.asList("1", "2", "3");
        DataStreamSource<String> streamSource = senv.fromCollection(list);
        //自定义函数
        SingleOutputStreamOperator<String> streamOperator = streamSource.flatMap(new MyFlatMap());

        streamOperator.print();


        senv.execute();
    }

    public static class MyFlatMap extends RichFlatMapFunction<String,String>{

        /**
         * 生命周期函数  全局调用一次
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("生命周期函数  全局调用一次");
            System.out.println(getRuntimeContext().getTaskName());
        }

        /**
         *  读取文件时会调用两次  其他均只会调用一次
         * @throws Exception
         */
        @Override
        public void close() throws Exception {

            System.out.println("生命周期函数 我挂了。。。。。");
        }

        /**
         * 处理逻辑
         * @param value
         * @param out
         * @throws Exception
         */
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] strings = value.split(" ");
            for (String string : strings) {
                out.collect(string);
            }
        }
    }
}
