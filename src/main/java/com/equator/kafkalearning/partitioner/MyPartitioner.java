package com.equator.kafkalearning.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * @Author: Equator
 * @Date: 2021/5/31 10:00
 **/
@Slf4j
public class MyPartitioner implements Partitioner {

    /**
     * 分区之前已经经过序列化
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // return key.hashCode() % cluster.partitionCountForTopic(topic);
        List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
        log.info("availablePartitions {}", availablePartitions);
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
