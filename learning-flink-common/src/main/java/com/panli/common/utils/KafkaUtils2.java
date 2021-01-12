package com.panli.common.utils;

import com.alibaba.fastjson.JSON;
import com.panli.domain.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author lipan
 * @date 2021-01-12
 */
public class KafkaUtils2 {

    private static final String broker_list = "localhost:9092";
    private static final String topic = "student";

    public static void writeToKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            Student student = new Student(i, "panli" + i, "pwd" + i, 18 + i);
            ProducerRecord record = new ProducerRecord(topic, null, null, JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据：" + JSON.toJSONString(student));
        }
        producer.flush();
    }

    public static void main(String[] args) {
        writeToKafka();
    }
}
