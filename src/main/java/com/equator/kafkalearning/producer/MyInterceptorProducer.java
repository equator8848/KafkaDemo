package com.equator.kafkalearning.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Author: Equator
 * @Date: 2021/5/30 11:11
 **/
@Slf4j
public class MyInterceptorProducer {
    public static void main(String[] args) {
        // 创建Kafka配置信息
        Properties conf = new Properties();
        // 重试次数
        conf.put(ProducerConfig.RETRIES_CONFIG, 3);
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092;kafka2:9093;kafka3:9094");
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 添加拦截器
        conf.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                Arrays.asList("com.equator.kafkalearning.interceptor.TimeInterceptor", "com.equator.kafkalearning.interceptor.CountInterceptor"));

        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);
        for (int i = 0; i < 8; i++) {
            producer.send(new ProducerRecord<>("mytopic", "MyProducerWithCallbackMsg" + i), (recordMetadata, e) -> {
                log.info("recordMetadata: partition: {}, offset: {}", recordMetadata.partition(), recordMetadata.offset(), e);
            });
        }
        producer.close();
    }
}
