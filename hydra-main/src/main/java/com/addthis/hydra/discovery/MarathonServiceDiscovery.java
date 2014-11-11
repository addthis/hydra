package com.addthis.hydra.discovery;

import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.GetAppResponse;
import mesosphere.marathon.client.model.v2.Task;

import java.util.ArrayList;
import java.util.Collection;

/**
 * simple utility service for discovering mesos services
 */
public class MarathonServiceDiscovery {

    public static String getHost(Marathon marathonClient, String appId) {
        GetAppResponse rabbitMQApp = marathonClient.getApp(appId);
        if (rabbitMQApp != null) {
            App rabbitMQ = rabbitMQApp.getApp();
            // always return first host
            // TODO: return random host?
            return rabbitMQ.getTasks().iterator().next().getHost();
        }
        // not found
        return null;
    }

    public static Collection<String> getHosts(Marathon marathonClient, String appId) {
        GetAppResponse rabbitMQApp = marathonClient.getApp(appId);
        Collection<String> hosts = new ArrayList<>();
        if (rabbitMQApp != null) {
            App rabbitMQ = rabbitMQApp.getApp();
            for (Task task : rabbitMQ.getTasks()) {
                hosts.add(task.getHost());
            }
        }
        return hosts;
    }

    public static Integer getAppPort(Marathon marathonClient, String appId, int portPosition) {
        // get rabbitmq task info
        GetAppResponse rabbitMQApp = marathonClient.getApp(appId);
        if (rabbitMQApp != null) {
            App rabbitMQ = rabbitMQApp.getApp();
            int mappedPortPosition = 0;
            for (Integer mappedPort : rabbitMQ.getPorts()) {
                if (portPosition == mappedPortPosition++) {
                    return mappedPort;
                }
            }
        }
        return -1;
    }

    public static String getHostsWithPort(Marathon marathonClient, String appId, int portPosition) {
        Collection<String> zkHosts = MarathonServiceDiscovery.getHosts(marathonClient, appId);
        int zkPort = MarathonServiceDiscovery.getAppPort(marathonClient, appId, portPosition);
        StringBuilder sb = new StringBuilder();
        for (String zkHost : zkHosts) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(zkHost).append(":").append(zkPort);
        }
        return sb.toString();
    }

}
