package com.admin.flink.day09;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink09_UDF_TableAggFun {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //TODO 4.不注册函数直接使用 TableAPI
//        table
//                .groupBy($("id"))
//                .flatAggregate(call(MyTop2Fun.class,$("vc")).as("value","top"))
//                .select($("id"),$("value"),$("top")).execute().print();

//        table
//                .groupBy($("id"))
//                .flatAggregate(call(MyTop2Fun.class,$("vc")))
//                .select($("id"),$("f0"),$("f1")).execute().print();


//        table
//                .groupBy($("id"))
//                .flatAggregate(call(MyTop2Fun.class, $("vc")).as("value", "top"))
//        .select($("id"),$("value"),$("top")).execute().print();
        //TODO 4.先注册再使用  TableAPI
        tableEnv.createTemporaryFunction("top2",MyTop2Fun.class);


        table
                .groupBy($("id"))
                .flatAggregate(call("top2", $("vc")).as("value", "top"))
                .select($("id"), $("value"), $("top")).execute().print();


    }

    //自定义一个累加器
    public static class MyTop2Accumulat {
        public Integer first;
        public Integer second;
    }

    //自定义表函数，求Top2
    public static class MyTop2Fun extends TableAggregateFunction<Tuple2<Integer, String>, MyTop2Accumulat> {


        @Override
        public MyTop2Accumulat createAccumulator() {
            MyTop2Accumulat myTop2Accumulat = new MyTop2Accumulat();
            myTop2Accumulat.first = Integer.MIN_VALUE;
            myTop2Accumulat.second = Integer.MIN_VALUE;
            return myTop2Accumulat;
        }

        public void accumulate(MyTop2Accumulat acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }


        public void emitValue(MyTop2Accumulat acc, Collector<Tuple2<Integer,String>> out){
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, "1"));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, "2"));
            }
        }

    }
}
