package com.addthis.hydra.kafka.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.addthis.hydra.kafka.KafkaUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.common.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.common.UnknownTopicOrPartitionException;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public final class ConsumerUtils {

    private static final Logger log = LoggerFactory.getLogger(ConsumerUtils.class);

    // example and default values taken from: https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
    public static Map<String, TopicMetadata> getTopicsMetadataFromBroker(SimpleConsumer consumer, List<String> topics) {
        TopicMetadataRequest request = new TopicMetadataRequest(topics);
        TopicMetadataResponse response = consumer.send(request);
        HashMap<String, TopicMetadata> topicsMetadata = new HashMap<>();
        for(TopicMetadata metadata : response.topicsMetadata()) {
            if(metadata.errorCode() != ErrorMapping.NoError()) {
                String message = "failed to get metadata for topic: " + metadata.topic() + ", from broker: " + consumer.host() + ":" + consumer.port();
                Throwable cause = ErrorMapping.exceptionFor(metadata.errorCode());
                log.error(message, cause);
                throw new RuntimeException(message, cause);
            }
            topicsMetadata.put(metadata.topic(), metadata);
        }
        return topicsMetadata;
    }

    public static Map<String, TopicMetadata> getTopicsMetadataFromBroker(String kafkaHost, int kafkaPort, List<String> topics) {
        SimpleConsumer consumer = null;
        try {
            consumer = new SimpleConsumer(kafkaHost, kafkaPort, 100000, 64 * 1024, "get-metadata");
            return getTopicsMetadataFromBroker(consumer, topics);
        } finally {
            if(consumer != null) {
                consumer.close();
            }
        }
    }

    public static Map<String, TopicMetadata> getTopicsMetadata(CuratorFramework zkClient, int seedBrokers, List<String> topics) {
        Iterator<Node> brokers = KafkaUtils.getSeedKafkaBrokers(zkClient, seedBrokers).values().iterator();
        Map<String, TopicMetadata> metadata = null;
        RuntimeException exception = new RuntimeException();
        // try to get metadata while we havent yet succeeded and still have more brokers to try
        while(metadata == null && brokers.hasNext()) {
            try {
                Node broker = brokers.next();
                metadata = getTopicsMetadataFromBroker(broker.host(), broker.port(), topics);
            } catch(Exception e) {
                exception.addSuppressed(e);
            }
        }
        if(metadata != null) {
            return metadata;
        }
        throw exception;
    }

    public static Map<String, TopicMetadata> getTopicsMetadata(String zookeepers, int seedBrokers, List<String> topics) {
        CuratorFramework zkClient = null;
        try {
            zkClient = KafkaUtils.newZkClient(zookeepers);
            return getTopicsMetadata(zkClient, seedBrokers, topics);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    public static TopicMetadata getTopicMetadata(CuratorFramework zkClient, int seedBrokers, String topic) {
        Map<String,TopicMetadata> metadatas = getTopicsMetadata(zkClient, seedBrokers, Collections.singletonList(topic));
        return metadatas.get(topic);
    }

    public static TopicMetadata getTopicMetadata(String zookeepers, int seedBrokers, String topic) {
        Map<String,TopicMetadata> metadatas = getTopicsMetadata(zookeepers, seedBrokers, Collections.singletonList(topic));
        return metadatas.get(topic);
    }

    public static Broker getNewLeader(SimpleConsumer consumer, String topic, int partition) {
        TopicMetadata metadata = getTopicsMetadataFromBroker(consumer, Collections.singletonList(topic)).get(topic);
        return metadata.partitionsMetadata().get(partition).leader();
    }

    // Also taken from wiki wholesale: https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
    // You have to wonder how this wrapper isn't included as part of the standard API...
    public static long getOffsetBefore(SimpleConsumer consumer, String topic, int partition, long whichTime) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), "get-offsets");
        OffsetResponse response = consumer.getOffsetsBefore(request);
        short errorCode = response.errorCode(topic, partition);
        if(errorCode == ErrorMapping.UnknownTopicOrPartitionCode() || errorCode == ErrorMapping.NotLeaderForPartitionCode()) {
            throw new UnknownTopicOrPartitionException("offset query failed - assuming partition has moved and trying again");
        }
        else if (response.hasError()) {
            Throwable exception = ErrorMapping.exceptionFor(response.errorCode(topic, partition));
            log.error("failed to get offset for {}-{} time {} from broker: {}:{}", topic, partition, whichTime,
                    consumer.host(), consumer.port(), ErrorMapping.exceptionFor(response.errorCode(topic, partition)));
            throw new RuntimeException(exception);
        }
        long[] offsets = response.offsets(topic, partition);
        if(offsets.length > 0) {
            return offsets[0];
        }
        // if no offsets before time are available, return earliest
        else if(whichTime != OffsetRequest.EarliestTime()) {
            return earliestOffsetAvailable(consumer, topic, partition);
        }
        // if request earliest offset returns nothing, then everything is completely borked
        else {
            throw new RuntimeException(String.format("earliest offset does not exist for %s-%d", topic, partition));
        }
    }

    public static long earliestOffsetAvailable(SimpleConsumer consumer, String topic, int partition) {
        return getOffsetBefore(consumer, topic, partition, OffsetRequest.EarliestTime());
    }

    public static long getOffsetBefore(SimpleConsumer consumer, String topic, int partition, long whichTime, int attempts){
        SimpleConsumer offsetConsumer = consumer;
        // try to query offset, starting with passed in consumer, but retrying with new connections to updated leader if previous attempts fail
        for(int i = 0; i < attempts; i++) {
            try {
                return getOffsetBefore(offsetConsumer, topic, partition, whichTime);
            } catch(UnknownTopicOrPartitionException e) {
                log.warn("offset query failed, assuming {}-{} has moved off of {}:{}, getting new leader, attempt: {}/{}",
                        topic, partition, offsetConsumer.host(), offsetConsumer.port(), i + 1, attempts);
                // close consumer if it is a temporary one created in this method
                if (offsetConsumer != consumer) {
                    offsetConsumer.close();
                }
                // create new consumer to current leader if more attempts remain
                if (i < attempts - 1) {
                    Broker leader = getNewLeader(consumer, topic, partition);
                    offsetConsumer = new SimpleConsumer(leader.host(), leader.port(), 100000, 64 * 1024, "get-offset");
                }
            }
        }
        throw new RuntimeException(String.format("offset query failed for %s-%d after %d attempts", topic, partition, attempts));
    }

    public static long earliestOffsetAvailable(SimpleConsumer consumer, String topic, int partition, int attempts) {
        return getOffsetBefore(consumer, topic, partition, OffsetRequest.EarliestTime(), attempts);
    }

    public static long latestOffsetAvailable(SimpleConsumer consumer, String topic, int partition, int attempts) {
        return getOffsetBefore(consumer, topic, partition, OffsetRequest.LatestTime(), attempts);
    }
}
