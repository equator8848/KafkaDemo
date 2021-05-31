package com.equator.kafkalearning.consumer;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * @Author: Equator
 * @Date: 2021/5/31 15:29
 **/

@Slf4j
public class MyConsumer {
    public static void main(String[] args) {
        // 创建Kafka配置信息
        Properties conf = new Properties();
        // 指定连接的Kafka集群
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092;kafka2:9093;kafka3:9094");
        // 自动提交与自动提交的延迟
        conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        conf.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // 指定序列化器
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 消费者组
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group_1");
        // 重置offset earliest（from beginning）|latest 前提是一个新的消费者或者offset已经过时
        conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(conf);
        // 订阅主题
        consumer.subscribe(Collections.singletonList("mytopic"));
        // 拉取数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                log.info("consumerRecord {} {} {}", consumerRecord.key(), consumerRecord.offset(), consumerRecord.value());
            }
        }
        // 关闭连接
        // consumer.close();
    }
}
