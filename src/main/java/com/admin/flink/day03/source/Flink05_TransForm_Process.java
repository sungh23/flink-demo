package com.admin.flink.day03.source;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author sungaohua
 */
public class Flink05_TransForm_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //将获取到的数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] words = value.split(" ");
                out.collect(new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2])));
            }
        });


//        KeyedStream<WaterSensor, Tuple> keyBy = waterSensorDStream.keyBy("id");
        KeyedStream<WaterSensor, String> keyBy = waterSensorDStream.keyBy(r -> r.getId());

        SingleOutputStreamOperator<WaterSensor> process = keyBy.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println(ctx.getCurrentKey());
                out.collect(new WaterSensor(value.getId() + "process", value.getTs(), value.getVc() + 1000));
            }
        });

        process.print();


        env.execute();

    }
}
