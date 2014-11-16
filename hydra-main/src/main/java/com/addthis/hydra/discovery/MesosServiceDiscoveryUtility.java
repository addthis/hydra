package com.addthis.hydra.discovery;

import com.addthis.basis.util.Parameter;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.MarathonClient;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.GetAppResponse;
import mesosphere.marathon.client.model.v2.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * simple utility service for discovering services deployed via Apache Marathon
 */
public class MesosServiceDiscoveryUtility {

    private static final String marathonURL = Parameter.value("mesos.marathonURL");
    private static final String proxy = Parameter.value("mesos.proxy");

    private static final Logger log = LoggerFactory.getLogger(MesosServiceDiscoveryUtility.class);

    public static Optional<App> getApp(Marathon marathonClient, String appId) {
        GetAppResponse marathonAppResponse = marathonClient.getApp(appId);
        if (marathonAppResponse != null) {
            return Optional.of(marathonAppResponse.getApp());
        }
        // not found
        return Optional.empty();
    }

    public static Optional<List<String>> getTaskHosts(String appId) {
        log.info(String.format("Loading task hosts for app %s via marathon %s mesosProxy %s", appId, marathonURL, proxy));
        Marathon marathon = MarathonClient.getInstance(marathonURL);
        Optional<App> marathonApp = MesosServiceDiscoveryUtility.getApp(marathon, appId);
        if (marathonApp.isPresent()) {
            if (log.isDebugEnabled()) {
                log.debug("Mesos App Info:\n" + marathonApp.get());
            }
            return Optional.of(marathonApp.get().getTasks().stream().map(Task::getHost).collect(Collectors.toList()));
        } else {
            throw new RuntimeException("Unable to find Marathon application: " + appId);
        }
    }

    public static int getAssignedPort(String appId, int portIndex) {
        log.info(String.format("Loading resource for app %s via marathon %s mesosProxy %s", appId, marathonURL, proxy));
        Marathon marathon = MarathonClient.getInstance(marathonURL);
        Optional<App> marathonApp = MesosServiceDiscoveryUtility.getApp(marathon, appId);
        if (marathonApp.isPresent()) {
            if (log.isDebugEnabled()) {
                log.debug("Mesos App Info:\n" + marathonApp.get());
            }
            return marathonApp.get().getPorts().get(portIndex);
        } else {
            throw new RuntimeException("Unable to find Marathon application: " + appId);
        }
    }

    public static String getMesosProxy() {
        return proxy;
    }

}
