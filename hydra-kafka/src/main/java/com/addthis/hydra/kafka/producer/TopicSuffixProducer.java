/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
