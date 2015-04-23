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

    public static Properties defaultConfig(String zookeeper, Properties overrides) {
        CuratorFramework zkClient = null;
        try {
            Properties properties = new Properties();
            zkClient = KafkaUtils.newZkClient(zookeeper);
            Collection<Node> brokers = KafkaUtils.getSeedKafkaBrokers(zkClient, 3).values();
            if (brokers.isEmpty()) {
                throw new RuntimeException("no kafka brokers available from zookeeper: " + zookeeper);
            }
            properties.put("bootstrap.servers", brokerListString(brokers));
            properties.put("request.required.acks", "all");
            properties.put("compression.codec", "gzip");
            return properties;
        } finally {
            if(zkClient != null) {
                zkClient.close();
            }
        }
    }

    public static Properties defaultConfig(String zookeeper) {
        return defaultConfig(zookeeper, new Properties());
    }

    public static Producer<Bundle,Bundle> newBundleProducer(String zookeeper, Properties overrides) {
        BundleEncoder encoder = new BundleEncoder();
        return new KafkaProducer<>(defaultConfig(zookeeper, overrides), encoder, encoder);
    }

    public static Producer<Bundle,Bundle> newBundleProducer(String zookeeper) {
        return newBundleProducer(zookeeper, new Properties());
    }

    public static Producer<Bundle,Bundle> newBundleProducer(String zookeeper, String topicSuffix, Properties overrides) {
        BundleEncoder encoder = new BundleEncoder();
        return new TopicSuffixProducer<>(new KafkaProducer<>(defaultConfig(zookeeper, overrides), encoder, encoder), topicSuffix);
    }

    public static Producer<Bundle,Bundle> newBundleProducer(String zookeeper, String topicSuffix) {
        return newBundleProducer(zookeeper, topicSuffix, new Properties());
    }
}
