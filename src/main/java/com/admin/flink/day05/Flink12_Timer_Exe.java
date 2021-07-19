package com.admin.flink.day05;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author sungaohua
 */
public class Flink12_Timer_Exe {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.对数据进行处理，封装成WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        //监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {

            //定义水位初始值
            private Integer lastVc = Integer.MIN_VALUE;

            // 定义定时器初始值
            private Long timer = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //判断水位是否上升
                if (lastVc < value.getVc()) {
                    // 上升  重新注册5秒定时器
                    if (timer == Long.MIN_VALUE) {
                        //
                        timer = ctx.timerService().currentProcessingTime() + 5000;
                        System.out.println("注册定时器");
                        ctx.timerService().registerProcessingTimeTimer(timer);
                    }
                } else {
                    // 没有连续上升
                    //删除定时器
                    System.out.println("删除定时器");
                    ctx.timerService().deleteProcessingTimeTimer(timer);
                    // 重置定时器时间
                    timer = Long.MIN_VALUE;

                }
                // 将当前水位保存  下次来比较
                lastVc = value.getVc();
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {

                ctx.output(new OutputTag<String>("output") {
                }, "水位连续上升。。" + ctx.getCurrentKey());

                // 重置定时器
                timer = Long.MIN_VALUE;
            }
        });

        process.print("主流：");
        process.getSideOutput(new OutputTag<String>("output"){}).print("侧输出流");

        env.execute();
    }
}
