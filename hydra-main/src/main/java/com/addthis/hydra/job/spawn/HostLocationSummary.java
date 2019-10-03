package com.addthis.hydra.job.spawn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.minion.HostLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostLocationSummary {
    private static final Logger log = LoggerFactory.getLogger(HostLocationSummary.class);

    private final AtomicReference<Map<String, Set<String>>> dataCenterToRackRef;
    private final AtomicReference<Map<String, Set<String>>> rackToPhysicalHostRef;

    public HostLocationSummary() {
        dataCenterToRackRef = new AtomicReference<>();
        rackToPhysicalHostRef = new AtomicReference<>();
    }

    /**
     * Rebuild host location summary with <i>current</i> list of live hosts
     * Then, Atomically update references holding host location summary info
     * @param hostStates Current list of live hosts as observed by HostManager
     */
    public void updateHostLocationSummary(List<HostState> hostStates) {
        Map<String, Set<String>> dataCenter = new HashMap<>();
        Map<String, Set<String>> rack = new HashMap<>();
        for(HostState host : hostStates) {
            if(host.isUp() && !host.isDead()) {
                HostLocation location = host.getHostLocation();
                String dc = location.getDataCenter();
                String rk = location.getRack();
                dataCenter.computeIfAbsent(dc, k -> new HashSet<>()).add(rk);
                rack.computeIfAbsent(rk, k -> new HashSet<>()).add(location.getPhysicalHost());
            }
        }
        dataCenterToRackRef.set(dataCenter);
        rackToPhysicalHostRef.set(rack);
    }

    /**
     * Choose the preferred HostLocation level (dataCenter, rack, physicalHost)
     * to maximize <i>distance/spread</i> across available HostLocation(s)
     * @return
     */
    public AvailabilityDomain getPriorityLevel() {
        Map<String, Set<String>> dataCenter = dataCenterToRackRef.get();
        if(dataCenter.size() > 1) {
            log.info("Priority Level: Datacenter ({})", dataCenter.size());
            return AvailabilityDomain.DATACENTER;
        }
        Map<String, Set<String>> rack = rackToPhysicalHostRef.get();
        if(rack.size() > 1) {
            log.info("Priority Level: Rack ({})", rack.size());
            return AvailabilityDomain.RACK;
        }
        if(!rack.isEmpty()) {
            Set<String> hosts = rack.entrySet().iterator().next().getValue();
            if (hosts.size() > 1) {
                log.info("Priority Level: Host ({})", hosts.size());
                return AvailabilityDomain.HOST;
            }
        }
        log.info("Priority Level: NONE");
        return AvailabilityDomain.NONE;
    }

    private int computeNumberOfHostsInRacks() {
        int hostCount = 0;
        for(Set<String> hostSet : rackToPhysicalHostRef.get().values()) {
            hostCount+= hostSet.size();
        }
        return hostCount;
    }

    public int getMinCardinality(AvailabilityDomain ad) {
        switch (ad) {
            case DATACENTER: return dataCenterToRackRef.get().size();
            case RACK: return rackToPhysicalHostRef.get().size();
            case HOST: return computeNumberOfHostsInRacks();
            case NONE:
            default: return 1;
        }
    }

}
