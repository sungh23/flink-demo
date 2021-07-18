package com.admin.flink.day04;

import com.admin.flink.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author sungaohua
 */
public class Flink05_Project_AppAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<MarketingUserBehavior> streamSource = env.addSource(new AppMarketingDataSource());
        
        //处理数据  不分渠道
        SingleOutputStreamOperator<Tuple2<String, Long>> map = streamSource.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(value.getBehavior(), 1L);
            }
        });

        //keyBy
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = map.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });


        tuple2StringKeyedStream.sum(1).print();


        env.execute();
    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}

