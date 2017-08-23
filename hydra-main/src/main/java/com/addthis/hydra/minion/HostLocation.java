package com.addthis.hydra.minion;

/**
 * holds physical location info of a minion. The info can be useful to decide where the replicas should be put, e.g.
 * replicas should be spread across different datacenters as much as possible, to minimize the risk of data loss
 */
public class HostLocation {
    private String dataCenter;
    private String rack;
    private String physicalHost; // a physical host can have many VMs
    private String hostname;

    public HostLocation(String dataCenter, String rack, String physicalHost, String hostname) {
        this.dataCenter = dataCenter;
        this.rack = rack;
        this.physicalHost = physicalHost;
        this.hostname = hostname;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public void setDataCenter(String dataCenter) {
        this.dataCenter = dataCenter;
    }

    public String getRack() {
        return rack;
    }

    public void setRack(String rack) {
        this.rack = rack;
    }

    public String getPhysicalHost() {
        return physicalHost;
    }

    public void setPhysicalHost(String physicalHost) {
        this.physicalHost = physicalHost;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }
}
