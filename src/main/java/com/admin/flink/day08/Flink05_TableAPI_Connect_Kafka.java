package com.admin.flink.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author sungaohua
 */
public class Flink05_TableAPI_Connect_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema().field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        // 连接kafka  读取数据
        tableEnv.connect(
                new Kafka()
                        .version("universal")  //设置版本
                        .topic("sink_sensor") // topic
                        .property("group.id", "bigdata0225") //消费者组
                        .startFromLatest() //消费模式  （从哪里开始消费）
                        .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092") //kafka连接地址
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 将临时表转换为table对象 为了调用相关算子
//        Table table = tableEnv.from("sensor");
//
//        Table select = table
//                .groupBy($("id"))
//                .aggregate($("vc").sum().as("count"))
//                .select($("id"),$("count"));


//        sql写法  不能有关键字冲突  或者用反引号包裹
        tableEnv.sqlQuery("select id , sum(vc) as `count` from sensor group by id").execute().print();

//        select.execute().print();




    }
}
