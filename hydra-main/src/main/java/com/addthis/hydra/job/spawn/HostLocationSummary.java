package com.addthis.hydra.job.spawn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.minion.HostLocation;

public class HostLocationSummary {

    private static Map<String, Set<String>> dataCenter;
    private static Map<String, Set<String>> rack;

    public enum HostLocationPriorityLevel {
        DATACENTER,
        RACK,
        HOST,
        NONE
    }

    public HostLocationSummary() {
        dataCenter = new HashMap<>();
        rack = new HashMap<>();
    }

    public void updateHostLocationSummary(HostState host) {
        HostLocation location = host.getHostLocation();
        String dc = location.getDataCenter();
        String rk = location.getRack();
        dataCenter.computeIfAbsent(dc, k -> new HashSet<>()).add(rk);
        rack.computeIfAbsent(rk, k-> new HashSet<>()).add(location.getPhysicalHost());
    }

    public HostLocationPriorityLevel getPriorityLevel() {
        if(dataCenter.size() > 1) {
            return HostLocationPriorityLevel.DATACENTER;
        }
        if(rack.size() > 1) {
            return HostLocationPriorityLevel.RACK;
        }
        Set<String> hosts = rack.entrySet().iterator().next().getValue();
        if(hosts.size() > 1) {
            return HostLocationPriorityLevel.HOST;
        }
        return HostLocationPriorityLevel.NONE;
    }

}
