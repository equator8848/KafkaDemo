package com.equator.kafkalearning.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * @Author: Equator
 * @Date: 2021/5/31 15:29
 **/

@Slf4j
public class MyOffsetCommitConsumer {
    public static void main(String[] args) {
        // 创建Kafka配置信息
        Properties conf = new Properties();
        // 指定连接的Kafka集群
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092;kafka2:9093;kafka3:9094");
        // 手动提交
        conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
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
            // 同步提交
            // consumer.commitSync();
            consumer.commitAsync((offsets, e) -> {
                if (e != null) {
                    log.error("offset commit error",e);
                }
            });
        }
    }
}
