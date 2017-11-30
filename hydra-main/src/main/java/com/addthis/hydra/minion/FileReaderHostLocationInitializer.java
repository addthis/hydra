package com.addthis.hydra.minion;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;

import org.codehaus.jackson.annotate.JsonCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileReaderHostLocationInitializer extends HostLocationInitializer{
    private static final Logger log = LoggerFactory.getLogger(FileReaderHostLocationInitializer.class);

    private File file;

    @JsonCreator
    public FileReaderHostLocationInitializer() {
        file = new File(System.getProperty("hostlocation.file","hostlocation.conf"));
        log.info("Using FileReaderHostLocationInitializer. Reading from {}", file.getName());
        if (!file.exists()) {
            log.warn("File {} does not exist", file.getName());
        }
    }

    @Override
    HostLocation getHostLocation() {
        String dataCenter = "Unknown";
        String rack = "Unknown";
        String physicalHost = "Unknown";
        try {
            Config config = ConfigFactory.parseFile(file);
            if (config.hasPath("dataCenter")) {
                dataCenter = config.getString("dataCenter");
            }
            if (config.hasPath("rack")) {
                rack = config.getString("rack");
            }
            if (config.hasPath("physicalHost")) {
                physicalHost = config.getString("physicalHost");
            }
        } catch (Exception e) {
            log.error("error getting host location from {}: {}", file.getName(), e);
        }
        return new HostLocation(dataCenter, rack, physicalHost);
    }
}
