package com.admin.flink.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;

/**
 * @author sungaohua
 */
public class Flink10_SQL_KafkaToKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从kafka读取数据 写到kafka

        //读  创建kafkaSource表
        tableEnv.executeSql("create table source_sensor (`id` STRING, `ts` BIGINT, `vc` INT) with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'topic_source_sensor1'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'format' = 'csv'" +
                ")");

        //写 创建kafkasink表
        tableEnv.executeSql("create table sink_sensor (id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_sink_sensor1',"
                + "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                + "'format' = 'csv'"
                + ")"
        );

        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id = 's1'");

        tableEnv.executeSql("select * from source_sensor").print();
        tableEnv.executeSql("select * from sink_sensor").print();

    }
}
