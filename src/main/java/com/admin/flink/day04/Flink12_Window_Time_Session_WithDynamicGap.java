package com.admin.flink.day04;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Flink12_Window_Time_Session_WithDynamicGap {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.对数据进行处理，封装成Tuple元组
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        //4.将相同元素的数据聚和到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");


        //TODO 5.开启基于时间会话窗口 ->动态会话间隔
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
            @Override
            public long extract(WaterSensor element) {
                return element.getTs()*1000;
            }
        }));


        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        window.process(new ProcessWindowFunction<WaterSensor, WaterSensor, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<WaterSensor> elements, Collector<WaterSensor> out) throws Exception {
                String start = sdf.format(new Date(context.window().getStart()));
                String end = sdf.format(new Date(context.window().getEnd()));
                System.out.println("窗口开始时间：" + start);
                System.out.println("窗口结束时间：" + end);
                for (WaterSensor waterSensor : elements) {
                    out.collect(waterSensor);
                }
            }
        }).print();

        env.execute();
    }
}
