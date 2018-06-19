package com.addthis.hydra.job.spawn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.minion.HostLocation;

public class HostLocationSummary {

    private final Map<String, Set<String>> dataCenter;
    private final Map<String, Set<String>> rack;

    public HostLocationSummary() {
        dataCenter = new HashMap<>();
        rack = new HashMap<>();
    }

    /**
     * Remove all existing mappings for dataCenter, rack and physicalHost info
     * Then rebuild host location summary with <i>current</i> list of live hosts
     * @param hostStates Current list of live hosts as observed by HostManager
     */
    public void updateHostLocationSummary(List<HostState> hostStates) {
        dataCenter.clear();
        rack.clear();
        for(HostState host : hostStates) {
            if(host.isUp() && !host.isDead()) {
                HostLocation location = host.getHostLocation();
                String dc = location.getDataCenter();
                String rk = location.getRack();
                dataCenter.computeIfAbsent(dc, k -> new HashSet<>()).add(rk);
                rack.computeIfAbsent(rk, k -> new HashSet<>()).add(location.getPhysicalHost());
            }
        }
    }

    public AvailabilityDomain getPriorityLevel() {
        if(dataCenter.size() > 1) {
            return AvailabilityDomain.DATACENTER;
        }
        if(rack.size() > 1) {
            return AvailabilityDomain.RACK;
        }
        if(!rack.isEmpty()) {
            Set<String> hosts = rack.entrySet().iterator().next().getValue();
            if (hosts.size() > 1) {
                return AvailabilityDomain.HOST;
            }
        }
        return AvailabilityDomain.NONE;
    }

    private int computeNumberOfHostsInRacks() {
        int hostCount = 0;
        for(Set<String> hostSet : rack.values()) {
            hostCount+= hostSet.size();
        }
        return hostCount;
    }

    public int getMinCardinality(AvailabilityDomain ad) {
        switch (ad) {
            case DATACENTER: return dataCenter.size();
            case RACK: return rack.size();
            case HOST: return computeNumberOfHostsInRacks();
            case NONE:
            default: return 0;
        }
    }

}
