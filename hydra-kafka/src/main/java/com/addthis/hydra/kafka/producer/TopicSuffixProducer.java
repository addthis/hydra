package com.addthis.hydra.kafka.producer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

public class TopicSuffixProducer<K,V> implements Producer<K,V> {

    private Producer<K,V> producer;
    private String topicSuffix;

    private ProducerRecord<K,V> wrapTopic(ProducerRecord<K,V> record) {
        return new ProducerRecord<K, V>(record.topic() + topicSuffix, record.partition(), record.key(), record.value());
    }

    public TopicSuffixProducer(Producer<K,V> producer, String topicSuffx) {
        this.producer = producer;
        this.topicSuffix = topicSuffx;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return this.producer.send(this.wrapTopic(record));
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        return this.producer.send(this.wrapTopic(record), callback);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return this.producer.partitionsFor(topic + topicSuffix);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return this.producer.metrics();
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
