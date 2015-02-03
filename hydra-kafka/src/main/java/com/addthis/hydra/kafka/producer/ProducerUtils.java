package com.addthis.hydra.kafka.producer;


import java.util.Collections;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;

import kafka.cluster.Broker;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.ZkUtils;
import scala.collection.Iterator;
import scala.collection.Seq;

public class ProducerUtils {

    public static String brokerListString(Seq<Broker> brokers) {
        Iterator<Broker> butwhowasforeach = brokers.iterator();
        StringBuilder stringBuilder = new StringBuilder();
        boolean first = true;
        while(butwhowasforeach.hasNext()) {
            Broker broker = butwhowasforeach.next();
            if(!first) {
                stringBuilder.append(",");
            }
            stringBuilder.append(broker.host()).append(":").append(broker.port());
            first = false;
        }
        return stringBuilder.toString();
    }

    public static ProducerConfig defaultConfig(String zookeeper, Properties overrides) {
        Properties properties = new Properties();
        ZkClient zkClient = new ZkClient(zookeeper);
        Seq<Broker> brokers = ZkUtils.getAllBrokersInCluster(zkClient);
        zkClient.close();
        properties.put("metadata.broker.list", brokerListString(brokers));
        properties.put("request.required.acks", "1");
        properties.put("producer.type", "async");
        properties.put("compression.codec", "gzip");
        properties.put("queue.buffering.max.messages", "0");
        for(Object name : Collections.list(overrides.propertyNames())) {
            properties.put(name, overrides.get(name));
        }
        return new ProducerConfig(properties);
    }

    public static ProducerConfig newConfig(String zookeeper) {
        return defaultConfig(zookeeper, new Properties());
    }

    public static <Key,Message> Producer<Key,Message> newProducer(String zookeeper, Properties overrides) {
        return new Producer<Key,Message>(defaultConfig(zookeeper, overrides));
    }

    public static <Key,Message> Producer<Key,Message> newProducer(String zookeeper) {
        return new Producer<Key,Message>(defaultConfig(zookeeper, new Properties()));
    }

    public static Producer newBundleProducer(String zookeeper) {
        Properties encoder = new Properties();
        encoder.setProperty("serializer.class", "com.addthis.hydra.kafka.producer.BundleEncoder");
        return new Producer(defaultConfig(zookeeper, encoder));
    }
}
