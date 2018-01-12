package com.addthis.hydra.minion;

import com.addthis.hydra.job.spawn.AvailabilityDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Holds physical location info of a minion. The info can be useful to decide where the replicas should be put, e.g.
 * replicas should be spread across different datacenters as much as possible, to minimize the risk of data loss
 */

public class HostLocation {
    private final String dataCenter;
    private final String rack;
    private final String physicalHost; // a physical host can have many VMs

    @JsonCreator
    public HostLocation(@JsonProperty("dataCenter") String dataCenter,
                 @JsonProperty("rack") String rack,
                 @JsonProperty("physicalHost") String physicalHost) {
        this.dataCenter = dataCenter;
        this.rack = rack;
        this.physicalHost = physicalHost;
    }

    public static HostLocation forHost(String hostname) {
        return new HostLocation("Unknown", "Unknown", hostname);
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public String getRack() {
        return rack;
    }

    public String getPhysicalHost() {
        return physicalHost;
    }

    /**
     * Get the HostLocation value corresponding to the given AvailabilityDomain.
     */
    public String getPriorityAd(AvailabilityDomain ad) {
        if (ad == AvailabilityDomain.DATACENTER) {
            return this.getDataCenter();
        } else if (ad == AvailabilityDomain.RACK) {
            return this.getRack();
        } else {
            // HOST or NONE or anything else
            return this.getPhysicalHost();
        }
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        HostLocation location = (HostLocation) obj;
        return location.getDataCenter().equals(this.dataCenter) &&
               location.getRack().equals(this.rack) &&
               location.getPhysicalHost().equals(this.physicalHost);
    }

    public String toString() {
        return "dataCenter=" + dataCenter + ", rack=" + rack + ", physicalHost=" + physicalHost;
    }

    /**
     * Returns a score of distance between this and another HostLocation.
     * Zero means maximum distance.
     * This method is used to sort HostLocations by their distance score.
     */
    public int assignScoreByHostLocation(HostLocation o) {
        if (this.getDataCenter().equals(o.getDataCenter())) {
            if (!this.getRack().equals(o.getRack())) {
                // Same dataCenter, different rack
                return AvailabilityDomain.RACK.score;
            }
            if (this.getPhysicalHost().equals(o.getPhysicalHost())) {
                // dataCenter, rack and physicalHost are the same
                return AvailabilityDomain.NONE.score;
            }
            // Same dataCenter, same rack, different physicalHost
            return AvailabilityDomain.HOST.score;
        }
        // Different dataCenter
        return AvailabilityDomain.DATACENTER.score;
    }
}
