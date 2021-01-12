package com.panli.data.sinks.customs.kafka;

import com.alibaba.fastjson.JSON;
import com.panli.domain.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author lipan
 * @date 2021-01-12
 * @desc 自定义 MySQL Sink
 */
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Student> studentStream = env.addSource(new FlinkKafkaConsumer011<>(
                "student",
                new SimpleStringSchema(),
                props
        )).setParallelism(1).map(string -> JSON.parseObject(string, Student.class));

        studentStream.addSink(new SinkToMySQL());

        env.execute("Run Flink Job With SinkToMySQL");
    }
}
