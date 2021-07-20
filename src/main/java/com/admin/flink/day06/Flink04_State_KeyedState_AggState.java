package com.admin.flink.day06;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
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
public class Flink04_State_KeyedState_AggState {
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

        //todo 计算每个传感器的平均水位
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, Tuple2<String,Double>>() {

            private AggregatingState<Integer, Double> aggState;

            @Override
            public void open(Configuration parameters) throws Exception {

                aggState= getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>
                        ("aggState", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {

                    /**
                     * 初始化累加器
                     * @return
                     */
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return Tuple2.of(0, 0);
                    }

                    /**
                     * 累加逻辑
                     * @param value
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                    }

                    /**
                     * 返回结果
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                        return accumulator.f0 * 1D / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return Tuple2.of(a.f0,b.f1);
                    }
                }, Types.TUPLE(Types.INT, Types.INT)));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String,Double>> out) throws Exception {

                aggState.add(value.getVc());


                out.collect(Tuple2.of(value.getId(),aggState.get()));

            }
        }).print();


        env.execute();
    }
}
