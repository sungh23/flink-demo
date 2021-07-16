package com.admin.flink.day03.sink;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 *  自定义Sink
 * @author sungaohua
 */
public class Flink04_Sink_Custom {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将从端口读古来的数据线转为waterSensor，在转为Json
        SingleOutputStreamOperator<WaterSensor> wDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        wDStream.addSink(new MyCustom());

        env.execute();

    }

    public static class MyCustom extends RichSinkFunction<WaterSensor>{

        private Connection connection;
        private PreparedStatement pstm;

        /**
         * 生命周期函数 初始化时执行
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {

            System.out.println("创建连接。。。。");

            // 创建Mysql连接   "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC","root","123456"
             connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");

            // 设置预执行语句
             pstm = connection.prepareStatement("insert into test value (?,?,?)");


        }

        /**
         * 生命周期函数 程序结束时调用
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("关闭连接。。。。");
            pstm.close();
            connection.close();
        }

        /**
         * 程序处理核心方法
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            pstm.setString(1,value.getId());
            pstm.setString(2,value.getTs().toString());
            pstm.setInt(3,value.getVc());

            // 执行语句
            pstm.execute();

            System.out.println("数据写入中");
        }
    }
}
