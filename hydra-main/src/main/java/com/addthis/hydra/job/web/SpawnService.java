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

import javax.servlet.ServletException;
import javax.websocket.DeploymentException;
import javax.websocket.server.ServerContainer;

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
import com.addthis.hydra.job.web.resources.GroupsResource;
import com.addthis.hydra.job.web.resources.HostResource;
import com.addthis.hydra.job.web.resources.JobsResource;
import com.addthis.hydra.job.web.resources.ListenResource;
import com.addthis.hydra.job.web.resources.MacroResource;
import com.addthis.hydra.job.web.resources.SearchResource;
import com.addthis.hydra.job.web.resources.SpawnConfig;
import com.addthis.hydra.job.web.resources.SystemResource;
import com.addthis.hydra.job.web.resources.TaskResource;
import com.addthis.hydra.job.web.websocket.SpawnWebSocket;
import com.addthis.hydra.util.PrometheusServletCreator;

import com.google.common.base.Charsets;
import com.google.common.io.Closer;
import com.google.common.io.Files;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.exporter.MetricsServlet;

public class SpawnService {

    private static final Logger log = LoggerFactory.getLogger(SpawnService.class);

    private static final int POLL_TIMEOUT = Integer.parseInt(System.getProperty("spawn.polltime", "1000"));
    private static final String WEB_DIR = Parameter.value("spawn.web.dir", "web");
    private static final String INDEX_FILENAME = Parameter.value("spawn.index.file", "index.html");
    private static final int SPAWN_RESOURCE_MAX_AGE_SECONDS = Parameter.intValue("spawn.web.resource.maxAge", 60 * 60 * 24);

    private final Server server;

    public SpawnService(final Spawn spawn, SpawnServiceConfiguration configuration) throws Exception {
        server = createServer(spawn, configuration);

    }

    public void start() throws Exception {
        PrometheusServletCreator.register();
        server.start();
    }

    private Server createServer(final Spawn spawn, SpawnServiceConfiguration configuration) throws Exception {
        Server server = new Server();

        initServerConnectors(server, spawn, configuration);

        // for closing certain api resource, namely JobsResource, but new ones can be added
        final Closer closer = Closer.create();
        ServletContextHandler context = createServletContextHandler(spawn, configuration, closer);
        Handler handler = createRootHandler(context);

        server.setAttribute("org.eclipse.jetty.Request.maxFormContentSize", 5000000);
        server.setHandler(handler);

        // this must be after server.setHandler(handler) because websocket configuration needs
        // the server object from servletContextHandler
        configureWebSocketServlet(context);

        server.addLifeCycleListener(new AbstractLifeCycle.AbstractLifeCycleListener() {
            @Override
            public void lifeCycleStopping(LifeCycle event) {
                super.lifeCycleStopping(event);
                try {
                    closer.close();
                } catch (IOException ex) {
                    log.error("IOException while closing jetty resources: ", ex);
                }
            }
        });
        return server;
    }

    private void initServerConnectors(Server server, Spawn spawn, SpawnServiceConfiguration configuration)
            throws IOException {

        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSecureScheme("https");
        httpConfig.setSecurePort(configuration.webPortSSL);

        // http
        ServerConnector http = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
        http.setPort(configuration.webPort);
        server.addConnector(http);

        // https
        if (configuration.requireSSL) {
            // using a SecureResquestCustomor is necessary to make ResoureHandler's welcome file redirect work
            // otherwise, https://[SPAWN_HOST]:5053 will be redirected to http://[SPAWN_HOST]:5053/index.html
            // see ATPS-499
            HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
            httpsConfig.addCustomizer(new SecureRequestCustomizer());
            SslConnectionFactory sslConnectionFactory = createSslConnectionFactory(configuration);
            ServerConnector https = new ServerConnector(server, sslConnectionFactory, new HttpConnectionFactory(httpsConfig));
            https.setPort(configuration.webPortSSL);
            server.addConnector(https);
            spawn.getSystemManager().updateSslEnabled(true);
        }
    }

    private SslConnectionFactory createSslConnectionFactory(SpawnServiceConfiguration configuration)
            throws IOException {
        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath(configuration.keyStorePath);
        sslContextFactory.setKeyStorePassword(readFile(configuration.keyStorePassword));
        sslContextFactory.setKeyManagerPassword(readFile(configuration.keyManagerPassword));
        return new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString());
    }

    private ServletContextHandler createServletContextHandler(Spawn spawn,
                                                              SpawnServiceConfiguration configuration,
                                                              Closer closer) {
        ServletContextHandler context = new ServletContextHandler();
        context.addServlet(new ServletHolder("metrics", new MetricsServlet()), "/metrics");
        context.addServlet(new ServletHolder("spawn-api", createApiServlet(spawn, configuration, closer)), "/*");
        return context;
    }

    private ServletContainer createApiServlet(Spawn spawn, SpawnServiceConfiguration configuration, Closer closer) {
        //instantiate resources
        SystemResource systemResource = new SystemResource(spawn);
        ListenResource listenResource = new ListenResource(spawn, systemResource, POLL_TIMEOUT);
        JobsResource jobsResource = new JobsResource(spawn, configuration, new JobRequestHandlerImpl(spawn));
        GroupsResource groupsResource = new GroupsResource(spawn, configuration);
        MacroResource macroResource = new MacroResource(spawn.getJobMacroManager());
        CommandResource commandResource = new CommandResource(spawn.getJobCommandManager());
        SearchResource searchResource = new SearchResource(spawn);
        TaskResource taskResource = new TaskResource(spawn);
        AliasResource aliasResource = new AliasResource(spawn);
        HostResource hostResource = new HostResource(spawn);
        AlertResource alertResource = new AlertResource(spawn.getJobAlertManager());
        AuthenticationResource authenticationResource = new AuthenticationResource(spawn);

        SpawnConfig resources = new SpawnConfig();
        resources.addResource(systemResource);
        resources.addResource(listenResource);
        resources.addResource(jobsResource);
        resources.addResource(groupsResource);
        resources.addResource(macroResource);
        resources.addResource(commandResource);
        resources.addResource(searchResource);
        resources.addResource(taskResource);
        resources.addResource(aliasResource);
        resources.addResource(hostResource);
        resources.addResource(alertResource);
        resources.addResource(authenticationResource);

        //register providers
        resources.addProvider(OptionalQueryParamInjectableProvider.class);
        resources.addProvider(KVPairsProvider.class);
        resources.addProvider(new JacksonJsonProvider(Jackson.defaultMapper()));

        //Feature settings
        resources.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

        closer.register(jobsResource);
        return new ServletContainer(resources);
    }

    private Handler createRootHandler(ServletContextHandler servletContextHandler) {
        ResourceHandler resourceHandler = createResourceHandler();

        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[]{resourceHandler, servletContextHandler});

        GzipHandler gzipHandler = new GzipHandler();
        gzipHandler.setHandler(handlers);

        return gzipHandler;
    }

    private ResourceHandler createResourceHandler() {
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setDirectoriesListed(true);
        resourceHandler.setWelcomeFiles(new String[]{INDEX_FILENAME});
        resourceHandler.setResourceBase(WEB_DIR);
        resourceHandler.setCacheControl("max-age=" + SPAWN_RESOURCE_MAX_AGE_SECONDS);
        return resourceHandler;
    }

    private void configureWebSocketServlet(ServletContextHandler servletContextHandler)
            throws ServletException, DeploymentException {
        ServerContainer wscontainer = WebSocketServerContainerInitializer.configureContext(servletContextHandler);
        wscontainer.addEndpoint(SpawnWebSocket.class);
    }

    private String readFile(String path) throws IOException {
        return Files.toString(new File(path), Charsets.UTF_8).trim();
    }

}
