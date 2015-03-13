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

import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public final class ConsumerUtils {

    private static final Logger log = LoggerFactory.getLogger(ConsumerUtils.class);

    // example and default values taken from: https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
    public static Map<String, TopicMetadata> getTopicsMetadataFromBroker(String kafkaHost, int kafkaPort, List<String> topics) {
        SimpleConsumer consumer = null;
        try {
            consumer = new SimpleConsumer(kafkaHost, kafkaPort, 100000, 64 * 1024, "get-topics-metadata");
            TopicMetadataRequest request = new TopicMetadataRequest(topics);
            TopicMetadataResponse response = consumer.send(request);
            HashMap<String, TopicMetadata> topicsMetadata = new HashMap<>();
            for(TopicMetadata metadata : response.topicsMetadata()) {
                topicsMetadata.put(metadata.topic(), metadata);
            }
            return topicsMetadata;
        } catch (Exception e) {
            log.error("failed to get topics metadata from broker, {}:{}; ", kafkaHost, kafkaPort, e);
            throw e;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public static Map<String, TopicMetadata> getTopicsMetadata(CuratorFramework zkClient, int seedBrokers, List<String> topics) throws Exception {
        Iterator<Node> brokers = KafkaUtils.getSeedKafkaBrokers(zkClient, seedBrokers).values().iterator();
        Map<String, TopicMetadata> metadata = null;
        Exception exception = null;
        // try to get metadata while we havent yet succeeded and still have more brokers to try
        while(metadata == null && brokers.hasNext()) {
            try {
                Node broker = brokers.next();
                metadata = getTopicsMetadataFromBroker(broker.host(), broker.port(), topics);
            } catch(Exception e) {
                exception = e;
            }
        }
        if(metadata != null) {
            return metadata;
        }
        throw exception;
    }

    public static Map<String, TopicMetadata> getTopicsMetadata(String zookeepers, int seedBrokers, List<String> topics) throws Exception {
        CuratorFramework zkClient = null;
        try {
            zkClient = KafkaUtils.newZkClient(zookeepers);
            return getTopicsMetadata(zookeepers, seedBrokers, topics);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    public static TopicMetadata getTopicMetadata(String zookeepers, int seedBrokers, String topic) throws Exception {
        Map<String,TopicMetadata> metadatas = getTopicMetadata(zookeepers, seedBrokers, Collections.singletonList(topic));
        TopicMetadata metadata = metadatas.get(topic);
        if(metadata == null) {
            throw new Exception("metadata request completed, but found no data for topic: " + topic + ", from zookeepers: " + zookeepers);
        }
        return metadata;
    }

}
