package com.admin.flink.day03.sink;

import com.admin.flink.bean.WaterSensor;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author sungaohua
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将从端口读古来的数据线转为waterSensor，在转为Json
        SingleOutputStreamOperator<String> jsonDStream = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return JSONObject.toJSONString(waterSensor);
            }
        });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092");

        FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer("testTopic", new SimpleStringSchema(), properties);

        //发送数据到kafka
        jsonDStream.addSink(kafkaProducer);
        jsonDStream.print();

        env.execute();
    }
}
