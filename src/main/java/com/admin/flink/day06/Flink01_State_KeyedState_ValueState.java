package com.admin.flink.day06;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author sungaohua
 */
public class Flink01_State_KeyedState_ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //todo 检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
        // 读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        KeyedStream<WaterSensor, Tuple> keyedStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
            // 因为需要使用键控状态 所以需要先进行keyBy操作
        }).keyBy("id");

        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {

            //定义初始状态
            private ValueState<Integer> lastVc;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态   从运行时上下文中获取初始状态
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("last-vc", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                // 获取状态
                int vc = lastVc.value()== null ? 0 : lastVc.value();

                if (Math.abs(vc-value.getVc())>10){
                    out.collect("警告 水位涨幅超过10");
                }
                //修改上一次状态
                lastVc.update(value.getVc());

            }
        }).print();


        env.execute();
    }
}
