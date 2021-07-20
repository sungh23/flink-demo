package com.admin.flink.day06;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author sungaohua
 */
public class Flink03_State_KeyedState_ReducingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        KeyedStream<WaterSensor, String> keyedStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
            // 因为需要使用键控状态 所以需要先进行keyBy操作
        }).keyBy(r->r.getId());

        //todo 计算每个传感器的水位和
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, Integer>() {

            // 定义状态
            private ReducingState<Integer> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态
                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("reducingState", new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        // 聚合逻辑
                        return value1 + value2;
                    }
                }, Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<Integer> out) throws Exception {

                // 添加元素
                reducingState.add(value.getVc());

                out.collect(reducingState.get());

            }
        }).print();


        env.execute();
    }
}
