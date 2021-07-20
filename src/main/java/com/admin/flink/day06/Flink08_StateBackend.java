package com.admin.flink.day06;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author sungaohua
 */
public class Flink08_StateBackend {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new MemoryStateBackend());

        env.setStateBackend(new FsStateBackend(""));

        env.setStateBackend(new RocksDBStateBackend(""));
    }
}
