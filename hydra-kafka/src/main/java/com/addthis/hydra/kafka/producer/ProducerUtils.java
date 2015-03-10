package com.addthis.hydra.kafka.producer;


import java.util.Collection;
import java.util.Properties;

import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.kafka.KafkaUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Node;

public class ProducerUtils {
    public static String brokerListString(Collection<Node> brokers) {
        StringBuilder stringBuilder = new StringBuilder();
        boolean first = true;
        for(Node broker : brokers) {
            if(!first) {
                stringBuilder.append(",");
            }
            stringBuilder.append(broker.host()).append(":").append(broker.port());
            first = false;
        }
        return stringBuilder.toString();
    }

    public static Properties defaultConfig(String zookeeper, Properties overrides) throws Exception {
        Properties properties = new Properties();
        CuratorFramework zkClient = KafkaUtils.newZkClient(zookeeper);
        Collection<Node> brokers = KafkaUtils.getKafkaBrokers(zkClient).values();
        zkClient.close();
        if(brokers.isEmpty()) {
            throw new Exception("failed to lookup kafka brokers from zookeeper: " + zookeeper);
        }
        properties.put("bootstrap.servers", brokerListString(brokers));
        properties.put("request.required.acks", "all");
        properties.put("compression.codec", "gzip");
        return properties;
    }

    public static Properties defaultConfig(String zookeeper) throws Exception {
        return defaultConfig(zookeeper, new Properties());
    }

    public static Producer<Bundle,Bundle> newBundleProducer(String zookeeper, Properties overrides) throws Exception {
        BundleEncoder encoder = new BundleEncoder();
        return new KafkaProducer<>(defaultConfig(zookeeper, overrides), encoder, encoder);
    }

    public static Producer<Bundle,Bundle> newBundleProducer(String zookeeper) throws Exception {
        return newBundleProducer(zookeeper, new Properties());
    }

    public static Producer<Bundle,Bundle> newBundleProducer(String zookeeper, String topicSuffix, Properties overrides) throws Exception {
        BundleEncoder encoder = new BundleEncoder();
        return new TopicSuffixProducer<>(new KafkaProducer<>(defaultConfig(zookeeper, overrides), encoder, encoder), topicSuffix);
    }

    public static Producer<Bundle,Bundle> newBundleProducer(String zookeeper, String topicSuffix) throws Exception {
        return newBundleProducer(zookeeper, topicSuffix, new Properties());
    }
}
