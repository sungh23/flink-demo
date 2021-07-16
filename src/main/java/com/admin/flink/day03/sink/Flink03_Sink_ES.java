package com.admin.flink.day03.sink;

import com.admin.flink.bean.WaterSensor;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;

import java.util.ArrayList;

/**
 * @author sungaohua
 */
public class Flink03_Sink_ES {
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

        // 创建list集合  存放 HttpHost 类型数据  （ES地址）
        ArrayList<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("hadoop102",9200));
        hosts.add(new HttpHost("hadoop103",9200));
        hosts.add(new HttpHost("hadoop104",9200));


        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(hosts, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {

                // 创建index请求，可以指定写入数据的索引 类型  id
                IndexRequest indexRequest = new IndexRequest("sensor", "_doc", element.getId());
                //将数据转换为json  并声明是json类型数据
                indexRequest.source(JSONObject.toJSONString(element), XContentType.JSON);

                // 添加
                indexer.add(indexRequest);
            }
        });

        // 流式数据 设置刷写入ES的条件  （一条刷写一次）
        waterSensorBuilder.setBulkFlushMaxActions(1);

        ElasticsearchSink<WaterSensor> esSink = waterSensorBuilder.build();

        wDStream.addSink(esSink);

        env.execute();
    }
}
