package com.admin.flink.day02.transform;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author sungaohua
 */
public class Flink01_Transform_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        // 读取数据
        DataStreamSource<String> streamSource = senv.socketTextStream("hadoop102", 9999);
        //TODO 3.使用map对数据进行转换，将读进来的数据封装成WaterSensor
        streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] words = value.split(" ");
                return new WaterSensor(words[0],Long.parseLong(words[1]),Integer.parseInt(words[2]));
            }
        }).print();


        senv.execute();
    }
}
