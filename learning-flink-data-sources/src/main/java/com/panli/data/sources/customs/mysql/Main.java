package com.panli.data.sources.customs.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lipan
 * @date 2021-01-12
 * @desc 自定义source
 */
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMySQL()).print();
        env.execute("Run Flink Job With MySQLSource");
    }
}
