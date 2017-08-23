package com.addthis.hydra.minion;

class HostLocationFromSystemProperty extends HostLocationInitializer{

    @Override HostLocation getHostLocation() {
        String dataCenter = System.getProperty("host.location.datacenter","none");
        String rack = System.getProperty("host.location.rack","none");
        String physicalHost = System.getProperty("host.physicalhost", "none");
        String hostname = System.getProperty("host.hostname", "none");

        return new HostLocation(dataCenter, rack, physicalHost, hostname);
    }
}
