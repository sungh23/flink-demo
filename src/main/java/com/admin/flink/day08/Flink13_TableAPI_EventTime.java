package com.admin.flink.day08;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author sungaohua
 */
public class Flink13_TableAPI_EventTime {
    public static void main(String[] args) {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> watermarks = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.
                <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs()*1000L;
                    }
                }));

//        SingleOutputStreamOperator<WaterSensor> streamOperator = env.fromElements(
//                new WaterSensor("sensor_1", 1000L, 10),
//                new WaterSensor("sensor_2", 7000L, 20),
//                new WaterSensor("sensor_2", 3000L, 30),
//                new WaterSensor("sensor_1", 4000L, 40),
//                new WaterSensor("sensor_1", 5000L, 50),
//                new WaterSensor("sensor_2", 6000L, 60))
//                .assignTimestampsAndWatermarks(WatermarkStrategy.
//                        <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
//                            @Override
//                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
//                                return element.getTs();
//                            }
//                        }));

        //创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //流转化为表 并指定事件时间字段  (使用rowTime指定)
        Table table = tEnv.fromDataStream(watermarks, $("id"), $("ts"), $("vc"), $("et").rowtime());
//        Table table = tEnv.fromDataStream(streamOperator, $("id"), $("ts").rowtime(), $("vc"));

//        tEnv.executeSql("select id ,sum (vc) as `cnt` from "+table+" group by id ").print();
        tEnv.executeSql("select * from " + table).print();

//        "select id sum(vc) cnt from "+table+"group by id "


    }
}
