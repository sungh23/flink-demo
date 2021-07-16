package com.admin.flink.day02.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author sungaohua
 */
public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        // 配置kafka相关属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "Flink01_Source_Kafka");
        // 设置kafka数据消费模式 （从当前位置开始消费 ）
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> sensor = senv.addSource(new FlinkKafkaConsumer<String>("testTopic", new SimpleStringSchema(), properties));

        sensor.print();

        senv.execute();


    }
}
