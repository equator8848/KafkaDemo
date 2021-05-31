package com.equator.kafkalearning.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author: Equator
 * @Date: 2021/5/30 11:11
 **/
@Slf4j
public class MyProducerWithPartitioner {
    public static void main(String[] args) {
        // 创建Kafka配置信息
        Properties conf = new Properties();
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092;kafka2:9093;kafka3:9094");
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 添加自定义分区器
        conf.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.equator.kafkalearning.partitioner.MyPartitioner");
        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);
        for (int i = 0; i < 8; i++) {
            producer.send(new ProducerRecord<>("mytopic", "MyProducerWithPartitionerMsg" + i), (recordMetadata, e) -> {
                log.info("recordMetadata: partition: {}, offset: {}", recordMetadata.partition(), recordMetadata.offset(), e);
            });
        }
        producer.close();
    }
}
