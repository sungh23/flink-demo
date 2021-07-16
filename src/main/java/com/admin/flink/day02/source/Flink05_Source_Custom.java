package com.admin.flink.day02.source;

import com.admin.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

/**
 * 自定义source
 *
 * @author sungaohua
 */
public class Flink05_Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        DataStreamSource<WaterSensor> streamSource = senv.addSource(new MySource());

        streamSource.print();


        senv.execute();
    }

    /**
     * 自定义source  需要手动实现SourceFunction 接口  或者 继承它的实现子类
     */
    public static class MySource extends RichSourceFunction<WaterSensor> {

        private Random random = new Random();

        private Boolean run = true;

        /**
         * 发送数据的方法
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (run) {
                ctx.collect(new WaterSensor("s1", 1L, random.nextInt(10)));
                Thread.sleep(2000);
            }
        }

        /**
         * 关闭数据，一般不自己调用
         */
        @Override
        public void cancel() {
            run = false;
        }
    }
}
