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
package com.addthis.hydra.job.web;

import java.io.File;
import java.io.IOException;

import com.addthis.basis.util.Parameter;

import com.addthis.codec.jackson.Jackson;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.web.jersey.KVPairsProvider;
import com.addthis.hydra.job.web.jersey.OptionalQueryParamInjectableProvider;
import com.addthis.hydra.job.web.resources.AlertResource;
import com.addthis.hydra.job.web.resources.AliasResource;
import com.addthis.hydra.job.web.resources.AuthenticationResource;
import com.addthis.hydra.job.web.resources.CommandResource;
import com.addthis.hydra.job.web.resources.HostResource;
import com.addthis.hydra.job.web.resources.JobsResource;
import com.addthis.hydra.job.web.resources.ListenResource;
import com.addthis.hydra.job.web.resources.MacroResource;
import com.addthis.hydra.job.web.resources.SpawnConfig;
import com.addthis.hydra.job.web.resources.SystemResource;
import com.addthis.hydra.job.web.resources.TaskResource;
import com.addthis.hydra.util.WebSocketManager;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.yammer.metrics.reporting.MetricsServlet;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpawnService {

    private static final Logger log = LoggerFactory.getLogger(SpawnService.class);
    private static final int batchInterval = Integer.parseInt(System.getProperty("spawn.batchtime", "500"));
    private static final int pollTimeout = Integer.parseInt(System.getProperty("spawn.polltime", "1000"));
    private static final String KEYSTORE_PATH = Parameter.value("spawn.https.keystore.path");
    private static final String KEYSTORE_PASSWORD = Parameter.value("spawn.https.keystore.password");
    private static final String KEYMANAGER_PASSWORD = Parameter.value("spawn.https.keymanager.password");

    private static final String webDir = Parameter.value("spawn.web.dir", "web");
    private static final String indexFilename = Parameter.value("spawn.index.file", "index.html");

    private final int webPort;
    private final int webPortSSL;
    private final Server jetty;
    private final SpawnConfig config;
    private final WebSocketManager webSocketManager;

    private static String readFile(String path) throws IOException {
        return Files.toString(new File(path), Charsets.UTF_8).trim();
    }

    public SpawnService(final Spawn spawn) throws Exception {
        this.jetty = new Server();
        this.webPort = spawn.getWebPort();
        this.webPortSSL = spawn.getWebPortSSL();

        SelectChannelConnector selectChannelConnector = new SelectChannelConnector();
        selectChannelConnector.setPort(webPort);


        if ((KEYSTORE_PASSWORD != null) && (KEYMANAGER_PASSWORD != null) && (KEYSTORE_PATH != null)) {
            SslSelectChannelConnector sslSelectChannelConnector = new SslSelectChannelConnector();
            sslSelectChannelConnector.setPort(webPortSSL);
            SslContextFactory sslContextFactory = sslSelectChannelConnector.getSslContextFactory();
            sslContextFactory.setKeyStorePath(KEYSTORE_PATH);
            sslContextFactory.setKeyStorePassword(readFile(KEYSTORE_PASSWORD));
            sslContextFactory.setKeyManagerPassword(readFile(KEYMANAGER_PASSWORD));
            log.info("Registering ssl connector");
            jetty.setConnectors(new Connector[]{selectChannelConnector, sslSelectChannelConnector});
            spawn.getSystemManager().updateSslEnabled(true);
        } else if (spawn.getRequireSSL()) {
            String message = "Missing one or more of \"spawn.https.keystore.path\", " +
                             "\"spawn.https.keystore.password\", and \"spawn.https.keymanager.password\". " +
                             "Set \"spawn.https.require\" to false to disable SSL.";
            throw new IllegalStateException(message);
        } else {
            log.info("Not registering ssl connector");
            spawn.getSystemManager().updateSslEnabled(false);
            jetty.setConnectors(new Connector[]{selectChannelConnector});
        }

        this.config = new SpawnConfig();
        this.webSocketManager = spawn.getWebSocketManager();

        //instantiate resources
        SystemResource systemResource = new SystemResource(spawn);
        ListenResource listenResource = new ListenResource(spawn, systemResource, pollTimeout);
        JobsResource jobsResource = new JobsResource(spawn, new JobRequestHandlerImpl(spawn));
        MacroResource macroResource = new MacroResource(spawn.getJobMacroManager());
        CommandResource commandResource = new CommandResource(spawn.getJobCommandManager());
        TaskResource taskResource = new TaskResource(spawn);
        AliasResource aliasResource = new AliasResource(spawn);
        HostResource hostResource = new HostResource(spawn);
        AlertResource alertResource = new AlertResource(spawn.getJobAlertManager());
        AuthenticationResource authenticationResource = new AuthenticationResource(spawn);

        //register resources
        config.addResource(systemResource);
        config.addResource(listenResource);
        config.addResource(jobsResource);
        config.addResource(macroResource);
        config.addResource(commandResource);
        config.addResource(taskResource);
        config.addResource(aliasResource);
        config.addResource(hostResource);
        config.addResource(alertResource);
        config.addResource(authenticationResource);

        //register providers
        config.addProvider(OptionalQueryParamInjectableProvider.class);
        config.addProvider(KVPairsProvider.class);
        config.addProvider(new JacksonJsonProvider(Jackson.defaultMapper()));

        //Feature settings
        config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    }

    public void start() throws Exception {
        log.warn("[init] running spawn2 on port " + webPort);

        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setDirectoriesListed(true);
        resourceHandler.setWelcomeFiles(new String[]{indexFilename});
        resourceHandler.setResourceBase(webDir);

        ServletContextHandler handler = new ServletContextHandler();
        webSocketManager.setHandler(handler);

        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[]{resourceHandler, webSocketManager});

        ServletContainer servletContainer = new ServletContainer(config);
        ServletHolder sh = new ServletHolder(servletContainer);

        handler.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
        handler.addServlet(sh, "/*");


        //jetty stuff
        jetty.setAttribute("org.eclipse.jetty.Request.maxFormContentSize", 5000000);
        //jetty.setHandler(webSocketManager);
        jetty.setHandler(handlers);
        jetty.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    jetty.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
