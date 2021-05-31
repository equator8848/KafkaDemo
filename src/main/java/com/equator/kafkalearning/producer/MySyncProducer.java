package com.equator.kafkalearning.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @Author: Equator
 * @Date: 2021/5/30 11:11
 **/
@Slf4j
public class MySyncProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 创建Kafka配置信息
        Properties conf = new Properties();
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092;kafka2:9093;kafka3:9094");
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);
        for (int i = 0; i < 8; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("mytopic", "MyProducerWithCallbackMsg" + i));
            RecordMetadata recordMetadata = future.get();
            log.info("recordMetadata: partition: {}, offset: {}", recordMetadata.partition(), recordMetadata.offset());
        }
        producer.close();
    }
}
