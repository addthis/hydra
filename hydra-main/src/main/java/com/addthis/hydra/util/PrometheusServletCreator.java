/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.hydra.util;

import java.io.File;

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
 * A wrapper util to create a prometheus metrics servlet, and register the prometheus JMX collector.
 */
public class PrometheusServletCreator {
    private static final Logger log = LoggerFactory.getLogger(PrometheusServletCreator.class);
    private static final String PROMETHEUS_CONFIG = ConfigFactory.load().getString("hydra.prometheus.config");

    /**
     * Create and add prometheus metrics servlet and add it to an existing handler; register the JMX collector;
     * add the handler to an existing server.
     * @param server  An existing jetty server.
     * @param handler An existing ServletContextHandler.
     */
    public static void create(Server server, ServletContextHandler handler) {
        create(handler);
        server.setHandler(handler);
    }

    /**
     * Create and add prometheus metrics servlet and add it to an existing handler; register the JMX collector.
     * @param handler An existing ServletContextHandler.
     */
    public static void create(ServletContextHandler handler) {
        handler.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
        register();
    }

    /**
     * register prometheus jmx collector.
     */
    public static void register() {
        try {
            File promConfig = new File(PROMETHEUS_CONFIG);
            if(promConfig.exists()) {
                new JmxCollector(promConfig).register();
                log.info("Using prometheus config file: {}", PROMETHEUS_CONFIG);
            } else {
                new JmxCollector("").register();
                log.warn("No prometheus config file found. Using prometheus default.");
            }
            DefaultExports.initialize();
            log.info("Prometheus collector registerd.");
        } catch (Exception e) {
            log.error("Prometheus collector not registerd: ", e);
        }

    }
}
