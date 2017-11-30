package com.addthis.hydra.minion;

import com.addthis.basis.util.Parameter;

import org.codehaus.jackson.annotate.JsonCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SystemPropertyHostLocationInitializer extends HostLocationInitializer{

    private static final Logger log = LoggerFactory.getLogger(SystemPropertyHostLocationInitializer.class);

    @JsonCreator
    public SystemPropertyHostLocationInitializer() {
        log.info("using SystemPropertyHostLocationInitializer");
    }

    @Override HostLocation getHostLocation() {
        String dataCenter = Parameter.value("hostlocation.datacenter", "Unknown");
        String rack = Parameter.value("hostlocation.rack","Unknown");
        String physicalHost = Parameter.value("hostlocation.physicalhost", "Unknown");

        return new HostLocation(dataCenter, rack, physicalHost);
    }
}
