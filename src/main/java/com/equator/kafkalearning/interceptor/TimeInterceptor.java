package com.equator.kafkalearning.interceptor;

import com.equator.kafkalearning.producer.MessageUtil;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author: Equator
 * @Date: 2021/6/1 9:55
 **/

public class TimeInterceptor implements ProducerInterceptor<String, String> {
    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String value = record.value() + "-" + MessageUtil.getMsg();

        return new ProducerRecord<>(record.topic(), record.partition(), record.key(), value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }
}
