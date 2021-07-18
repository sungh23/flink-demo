package com.admin.flink.day04;

import com.admin.flink.bean.OrderEvent;
import com.admin.flink.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author sungaohua
 */
public class Flink07_Project_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> orderStreamSource = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> receStreamSource = env.readTextFile("input/ReceiptLog.csv");

        //转换成javaBean
//3.分别将两个流的数据转为JavaBean
        SingleOutputStreamOperator<OrderEvent> orderDStream = orderStreamSource.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        SingleOutputStreamOperator<TxEvent> receDStream = receStreamSource.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2])
                );
            }
        });

        //连接两条流  connect 直接连接 不考虑流格式
        ConnectedStreams<OrderEvent, TxEvent> connect = orderDStream.connect(receDStream);

        // 对相同交易码的数据聚合到一起
        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = connect.keyBy("txId", "txId");


        connectedStreams.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {

            //定义map 临时存放数据
            HashMap<String, OrderEvent> orderMap = new HashMap<>();
            HashMap<String, TxEvent> eventMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                // 判断数据是否存在
                if (eventMap.containsKey(value.getTxId())) {
                    System.out.println("订单" + value.getOrderId() + "对账成功");
                    //清楚数据
                    eventMap.remove(value.getTxId());
                }else {
                    //存入到临时集合中
                    orderMap.put(value.getTxId(),value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                // 判断数据是否存在
                if (orderMap.containsKey(value.getTxId())) {
                    System.out.println("订单" + orderMap.get(value.getTxId()).getOrderId() + "对账成功");
                    //清楚数据
                    orderMap.remove(value.getTxId());
                }else {
                    //存入到临时集合中
                    eventMap.put(value.getTxId(),value);
                }
            }
        }).print();




        env.execute();
    }
}
