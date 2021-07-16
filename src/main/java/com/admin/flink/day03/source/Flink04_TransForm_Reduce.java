package com.admin.flink.day03.source;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author sungaohua
 */
public class Flink04_TransForm_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //将获取到的数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

//        KeyedStream<WaterSensor, String> keyBy = waterSensorDStream.keyBy(r -> r.getId());
        KeyedStream<WaterSensor, Tuple> keyBy = waterSensorDStream.keyBy("id");

        keyBy.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                // v1 是老的元素  v2 是新的那条数据，且第一条数据不会进入reduce方法
                System.out.println("reduce.....");
                return new WaterSensor(value1.getId(),value2.getTs(),Math.max(value1.getVc(),value2.getVc()));
            }
        }).print();


        env.execute();

    }
}
