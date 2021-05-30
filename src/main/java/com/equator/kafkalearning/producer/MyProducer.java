package com.equator.kafkalearning.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author: Equator
 * @Date: 2021/5/30 11:11
 **/

public class MyProducer {
    public static void main(String[] args) {
        // 创建Kafka配置信息
        Properties conf = new Properties();
        // 指定连接的Kafka集群
        conf.put("bootstrap.servers", "kafka1:9092");
        // ack策略
        conf.put("acks", "all");
        // 重试次数
        conf.put("retries", 3);
        // 批次大小 16 k
        conf.put("batch.size", 16 * 1024);
        // 攒batch等待时间
        conf.put("linger.ms", 1);
        // RecordAccumulator缓冲区大小
        conf.put("buffer.memory", 32 * 1024 * 1024);
        // 指定序列化器
        conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        conf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);
        for (int i = 0; i < 8; i++) {
            producer.send(new ProducerRecord<>("mytopic", "msg" + i));
        }
        // 如果没有关闭资源，数据不够batch或者时间小于攒batch时数据会被清空不会被发送出去
        producer.close();
    }
}
