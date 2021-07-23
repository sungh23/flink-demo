package com.admin.flink.day08;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author sungaohua
 */
public class Flink09_SQl_Login {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转换为临时表  ->未注册的表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //  注册表
        tableEnv.createTemporaryView("sensor",table);

        // 直接从数据流注册表
        tableEnv.createTemporaryView("sensor2",waterSensorStream);

        //查询

        tableEnv.executeSql("select * from sensor2").print();




    }
}
