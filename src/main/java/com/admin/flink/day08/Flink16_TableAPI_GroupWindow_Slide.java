package com.admin.flink.day08;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author sungaohua
 */
public class Flink16_TableAPI_GroupWindow_Slide {
    public static void main(String[] args) {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 5000L, 40),
                new WaterSensor("sensor_1", 6000L, 50),
                new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );


        //2.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3 将流转化为表 并指定事件时间
        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts"), $("vc"), $("t").rowtime());

        // 开启基于时间的滑动窗口
        Table select = table
                // 指定窗口大小和滑动步长   参数一  窗口大小 参数二 滑动步长  窗口左开右闭的
                .window(Slide.over(lit(4).second()).every(lit(2).second()).on($("t")).as("w"))
//                .window(Tumble.over(lit(5).second()).on($("t")).as("w"))
                //分组时一定要把窗口函数放入
                .groupBy($("id"), $("w"))
                .select($("id"),$("vc").sum());

        select.execute().print();


    }
}
