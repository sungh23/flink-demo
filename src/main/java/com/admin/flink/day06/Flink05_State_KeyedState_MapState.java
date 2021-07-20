package com.admin.flink.day06;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author sungaohua
 */
public class Flink05_State_KeyedState_MapState {
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
        }).keyBy(r -> r.getId());

        //todo 去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            private MapState<Integer, WaterSensor> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>("mapState", Integer.class, WaterSensor.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                if (!mapState.contains(value.getVc())) {
                    out.collect(value);
                    mapState.put(value.getVc(),value);
                }

            }
        }).print();


        env.execute();
    }
}
