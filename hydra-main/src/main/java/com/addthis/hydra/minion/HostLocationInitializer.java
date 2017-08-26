package com.addthis.hydra.minion;

// import org.codehaus.jackson.map.annotate.JsonDeserialize;

/**
 * Provides the method to get host location
 */
// @JsonDeserialize(as = SystemPropertyHostLocationInitializer.class)
abstract class HostLocationInitializer {

    abstract HostLocation getHostLocation();
}
