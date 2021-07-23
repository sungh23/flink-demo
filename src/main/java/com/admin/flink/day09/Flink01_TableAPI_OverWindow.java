package com.admin.flink.day09;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author sungaohua
 */
public class Flink01_TableAPI_OverWindow {
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
                new WaterSensor("sensor_2", 6000L, 60),
                new WaterSensor("sensor_2", 7000L, 60))
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

        // 3 将流转化为表 并指定处理时间
        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts"), $("vc"), $("t").rowtime());

        // 进行开窗操作  求vc的和
        Table select = table
                //上无边界到当前行
//                .window(Over.partitionBy($("id")).orderBy($("t")).as("w"))
                //从当前行往前两秒钟
//                .window(Over.partitionBy($("id")).orderBy($("t")).preceding(lit(2).second()).as("w"))
                // 从当前行  往前两个元素 跟时间无关
                .window(Over.partitionBy($("id")).orderBy($("t")).preceding(rowInterval(2L)).as("w"))
                .select($("id"),$("ts"),$("vc"),$("vc").sum().over($("w")));

        select.execute().print();


    }
}
