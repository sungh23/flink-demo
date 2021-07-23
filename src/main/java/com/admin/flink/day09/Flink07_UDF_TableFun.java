package com.admin.flink.day09;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink07_UDF_TableFun {
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
//                .leftOuterJoinLateral(call(MySplitFun.class,$("id")))
//                .select($("id"),$("word")).execute().print();

        //TODO 4.先注册再使用  TableAPI
        tableEnv.createTemporaryFunction("mySql", MySplitFun.class);

//       table
//               .joinLateral(call("mySpl",$("id")))
//               .select($("id"),$("word")).execute().print();

        //sql 写法
//        tableEnv.executeSql("select id,word from " + table + " join Lateral table (mySql(id)) on true").print();
//        tableEnv.executeSql("select id,word from " + table + " join Lateral table (mySql(id)) on true").print();

        tableEnv.executeSql("select id,word2 from "+table+", LATERAL TABLE (mySql(id))").print();


        //SQL 写法
//        tableEnv.executeSql("select id,word from " + table + ", Lateral table (myTableFun(id))").print();

    }

    //自定义表函数，按照下划线切分id
    @FunctionHint(output = @DataTypeHint("ROW<word2 STRING>"))
    public static class MySplitFun extends TableFunction<Row> {
        public void eval(String value) {
            String[] split = value.split("_");
            for (String word : split) {
                collect(Row.of(word));
            }
        }
    }

}
