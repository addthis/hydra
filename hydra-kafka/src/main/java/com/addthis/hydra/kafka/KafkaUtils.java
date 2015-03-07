package com.addthis.hydra.kafka;

import java.io.IOException;

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

/**
 * Created by steve on 3/6/15.
 */
public class KafkaUtils {

    public static final String brokersPath = "/brokers/ids";

    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();


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

    public static Map<Integer, Node> getKafkaBrokers(CuratorFramework zkClient) throws Exception {
        Map<Integer, Node> brokers = new HashMap<>();
        List<String> ids = zkClient.getChildren().forPath("/brokers/ids");
        for (String id : ids) {
            try {
                int intId = Integer.parseInt(id);
                byte[] brokerInfo = zkClient.getData().forPath(brokersPath + '/' + intId);
                brokers.put(intId, parseBrokerInfo(intId, brokerInfo));
            } catch (Exception e) {
                log.error("failed to get/parse broker info for broker: " + id, e);
            }
        }
        return brokers;
    }
}
