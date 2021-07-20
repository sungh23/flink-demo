package com.admin.flink.day06;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author sungaohua
 */
public class Flink02_State_KeyedState_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


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

        //todo 针对每个传感器输出最高的3个水位值  使用listState
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, List<Integer>>() {

            //定义状态
            private ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState", Integer.class));

            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<List<Integer>> out) throws Exception {

                //将当前数据保存到listState中
                listState.add(value.getVc());

                ArrayList<Integer> list = new ArrayList<>();

                // 取出前三的数据
                Iterable<Integer> integers = listState.get();
                for (Integer integer : integers) {
                    list.add(integer);
                }

                //排序
                list.sort((o1,o2)->o2-o1);

                // 取前三数据
                if (list.size()>3){
                    list.remove(3);
                }

                //更新数据
                listState.update(list);

                out.collect(list);

            }
        }).print();


        env.execute();
    }
}
