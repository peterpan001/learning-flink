package com.panli.common.utils;

import com.alibaba.fastjson.JSON;
import com.panli.domain.Metric;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author lipan
 * @date 2021-01-12
 * @desc Kafka工具类
 */
public class KafkaUtils {

    private static final String broker_list = "localhost:9092";
    private static final String topic = "metric";

    public static void writeToKafka() {

        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // key序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // value序列化

        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Metric metric = new Metric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("memory");
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        tags.put("cluster", "panli");
        tags.put("host_ip", "192.168.0.1");
        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);
        metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(metric));
        producer.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(metric));

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            writeToKafka();
        }
    }
}
