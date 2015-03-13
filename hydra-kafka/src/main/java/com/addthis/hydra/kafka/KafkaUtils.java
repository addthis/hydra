package com.addthis.hydra.kafka;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.common.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaUtils {

    public static final String brokersPath = "/brokers/ids";

    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private KafkaUtils() {
        // intellij made me do it (so people dont instantiate utils class)
    }

    public static CuratorFramework newZkClient(String zookeepers) {
        CuratorFramework framework = CuratorFrameworkFactory.builder()
                .sessionTimeoutMs(600000)
                .connectionTimeoutMs(180000)
                .connectString(zookeepers)
                .retryPolicy(new ExponentialBackoffRetry(1000, 25))
                .defaultData(null)
                .build();
        framework.start();
        return framework;
    }

    public static Node parseBrokerInfo(int id, byte[] brokerInfo) throws IOException {
        Map<String,Object> map = (Map)objectMapper.readValue(brokerInfo, Object.class);
        return new Node(id, (String)map.get("host"), (Integer)map.get("port"));
    }

    public static List<Integer> getKafkaBrokerIds(CuratorFramework zkClient) throws Exception {
        List<String> ids = zkClient.getChildren().forPath("/brokers/ids");
        List<Integer> intIds = new ArrayList<>();
        for (String id : ids) {
            try {
                int intId = Integer.parseInt(id);
                intIds.add(intId);
            } catch (Exception e) {
                log.warn("ignoring invalid broker id: " + id, e);
            }
        }
        return intIds;
    }

    public static Map<Integer, Node> getSeedKafkaBrokers(CuratorFramework zkClient, int brokerCount) throws Exception {
        Map<Integer, Node> brokers = new HashMap<>();
        List<Integer> ids = getKafkaBrokerIds(zkClient);
        Collections.shuffle(ids);
        brokerCount = (brokerCount == -1 ? ids.size() : Math.min(brokerCount, ids.size()));
        for (int id : ids.subList(0, brokerCount)) {
            try {
                byte[] brokerInfo = zkClient.getData().forPath(brokersPath + '/' + id);
                brokers.put(id, parseBrokerInfo(id, brokerInfo));
            } catch (Exception e) {
                log.warn("failed to get/parse broker info for broker: " + id, e);
            }
        }
        return brokers;
    }

    public static Map<Integer, Node> getKafkaBrokers(CuratorFramework zkClient) throws Exception {
        return getSeedKafkaBrokers(zkClient, -1);
    }
}

