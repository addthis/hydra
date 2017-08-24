package com.addthis.hydra.minion;

import org.codehaus.jackson.annotate.JsonCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SystemPropertyHostLocationInitializer extends HostLocationInitializer{

    private static final Logger log = LoggerFactory.getLogger(Minion.class);

    @JsonCreator
    public SystemPropertyHostLocationInitializer() {
        log.info("using SystemPropertyHostLocationInitializer");
    }

    @Override HostLocation getHostLocation() {
        String dataCenter = System.getProperty("host.location.datacenter","none");
        String rack = System.getProperty("host.location.rack","none");
        String physicalHost = System.getProperty("host.physicalhost", "none");

        return new HostLocation(dataCenter, rack, physicalHost);
    }
}
