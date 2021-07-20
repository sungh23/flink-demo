package com.admin.flink.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_State_OperatorState_Broadcast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //获取两条流
        DataStreamSource<String> localDStream = env.socketTextStream("hadoop102", 8888);

        DataStreamSource<String> hadoopDStream = env.socketTextStream("hadoop102", 9999);
        
        //广播一条流
        MapStateDescriptor<String, String> broadcast = new MapStateDescriptor<>("broadcast", String.class, String.class);
        BroadcastStream<String> broadcastStream = hadoopDStream.broadcast(broadcast);

        BroadcastConnectedStream<String, String> connect = localDStream.connect(broadcastStream);

        //通过一条流的数据来决定另一个流的逻辑
        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取广播状态
                ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(broadcast);
                if ("1".equals(state.get("switch"))) {
                    out.collect("执行逻辑1。。。。");
                } else if ("2".equals(state.get("switch"))) {
                    out.collect("执行逻辑2。。。。");
                } else {
                    out.collect("执行其他逻辑");
                }

            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {

                //获取广播状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcast);

                //存放广播数据
                broadcastState.put("switch",value);


            }
        }).print();

        env.execute();

    }
}
