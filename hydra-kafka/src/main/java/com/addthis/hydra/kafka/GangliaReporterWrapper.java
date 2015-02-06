package com.addthis.hydra.kafka;

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.reporting.GangliaReporter;

import org.slf4j.LoggerFactory;

import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;

/**
 * Created by steve on 2/5/15.
 */
public class GangliaReporterWrapper implements KafkaMetricsReporter {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(GangliaReporterWrapper.class);

    GangliaReporter reporter;

    public GangliaReporterWrapper() {
        // under construction
    }

    @Override
    public void init(VerifiableProperties properties) {
        try {
            String gangliaHost = properties.getProperty("ganglia.host");
            Integer gangliaPort = Integer.parseInt(properties.getProperty("ganglia.port"));
            reporter = new GangliaReporter(gangliaHost, gangliaPort);
            reporter.start(Integer.parseInt(properties.getProperty("ganglia.intervalMs")), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("failed to init ganglia reporter, ganglia.host=" + properties.getProperty("ganglia.host") +
                      ", ganglia.port=" + properties.getProperty("ganglia.port") + ", ganglia.intervalMs=" + properties.getProperty("ganglia.intervalMs") + " : " + e);
        }

    }
}
