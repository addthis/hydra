package com.addthis.hydra.util;

import java.io.File;

import com.addthis.basis.util.Parameter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import io.prometheus.jmx.JmxCollector;

/**
 * Create prometheus web servlet, and add it to a jetty server.
 */
public class PrometheusServletCreator {
    private static final Logger log = LoggerFactory.getLogger(PrometheusServletCreator.class);
    private static final String PROMETHEUS_CONFIG = ConfigFactory.load().getString("hydra.prometheus.config");
    /**
     *
     * @param server    An existing jetty server.
     * @param handler   An existing ServletContextHandler.
     * @return
     */
    public static void create(Server server, ServletContextHandler handler) {
        handler.addServlet(new ServletHolder(new MetricsServlet()), "/prometheus");
        server.setHandler(handler);
        register();
    }

    /**
     *
     * register prometheus jmx collector
     */
    private static void register() {
        try {
            new JmxCollector(new File(PROMETHEUS_CONFIG)).register();
            DefaultExports.initialize();
            log.info("Registered prometheus metrics based on rule file:", PROMETHEUS_CONFIG);
        } catch (Exception e) {
            log.warn("Prometheus collector not registerd: ", e);
        }

    }
}
