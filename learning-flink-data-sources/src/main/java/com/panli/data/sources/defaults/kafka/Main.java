package com.panli.data.sources.defaults.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author lipan
 * @date 2021-01-12
 * @desc Flink 使用 kafka 作为 source
 */
public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); //value 反序列化

        DataStreamSource<String> datasource = env.addSource(new FlinkKafkaConsumer011<>(
                "metric", //kafka topic
                new SimpleStringSchema(), // String序列化
                props
        )).setParallelism(1);

        datasource.print(); // 读取到的kafka数据打印控制台

        env.execute("Run Flink Job with Kafka Datasource.");
    }
}
