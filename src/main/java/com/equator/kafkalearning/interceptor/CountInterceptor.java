package com.equator.kafkalearning.interceptor;

import com.equator.kafkalearning.producer.MessageUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author: Equator
 * @Date: 2021/6/1 9:55
 **/

@Slf4j
public class CountInterceptor implements ProducerInterceptor<String, String> {

    int success = 0;
    int error = 0;

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            success++;
        } else {
            error++;
        }
    }

    @Override
    public void close() {
        log.info("success {} error {}", success, error);
    }
}
