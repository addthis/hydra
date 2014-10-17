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

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.web.jersey.BasicAuthProvider;
import com.addthis.hydra.job.web.jersey.DefaultAuthenticator;
import com.addthis.hydra.job.web.jersey.KVPairsProvider;
import com.addthis.hydra.job.web.jersey.OptionalQueryParamInjectableProvider;
import com.addthis.hydra.job.web.jersey.SpawnAuthProvider;
import com.addthis.hydra.job.web.jersey.User;
import com.addthis.hydra.job.web.resources.AlertResource;
import com.addthis.hydra.job.web.resources.AliasResource;
import com.addthis.hydra.job.web.resources.CommandResource;
import com.addthis.hydra.job.web.resources.HostResource;
import com.addthis.hydra.job.web.resources.JobsResource;
import com.addthis.hydra.job.web.resources.ListenResource;
import com.addthis.hydra.job.web.resources.MacroResource;
import com.addthis.hydra.job.web.resources.SpawnConfig;
import com.addthis.hydra.job.web.resources.SystemResource;
import com.addthis.hydra.job.web.resources.TaskResource;
import com.addthis.hydra.util.WebSocketManager;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.yammer.dropwizard.auth.Authenticator;
import com.yammer.metrics.reporting.MetricsServlet;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class SpawnService {

    private static final Logger log = LoggerFactory.getLogger(SpawnService.class);
    private static final int batchInterval = Integer.parseInt(System.getProperty("spawn.batchtime", "500"));
    private static final int pollTimeout = Integer.parseInt(System.getProperty("spawn.polltime", "1000"));
    private static final String defaultUser = "UNKNOWN_USER";
    private static final int webPort = Parameter.intValue("spawn.http.port", 5052);

    private static final boolean useBasicAuth = Parameter.boolValue("spawn.basic.auth", false);
    private static final String webDir = Parameter.value("spawn.web.dir", "web");
    private static final String indexFilename = Parameter.value("spawn.index.file", "index.html");

    private final Server jetty;
    private final SpawnConfig config;
    private final WebSocketManager webSocketManager;

    public SpawnService(final Spawn spawn) throws Exception {
        this.jetty = new Server(webPort);
        this.config = new SpawnConfig();
        this.webSocketManager = spawn.getWebSocketManager();

        //instantiate resources
        SystemResource systemResource = new SystemResource(spawn.getSystemManager());
        ListenResource listenResource = new ListenResource(spawn, systemResource, pollTimeout);
        JobsResource jobsResource = new JobsResource(spawn, new JobRequestHandlerImpl(spawn));
        MacroResource macroResource = new MacroResource(spawn.getJobMacroManager());
        CommandResource commandResource = new CommandResource(spawn.getJobCommandManager());
        TaskResource taskResource = new TaskResource(spawn);
        AliasResource aliasResource = new AliasResource(spawn.getAliasManager());
        HostResource hostResource = new HostResource(spawn);
        AlertResource alertResource = new AlertResource(spawn.getJobAlertManager());

        //authenticator
        Authenticator authenticator = new DefaultAuthenticator();

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

        //register providers
        config.addProvider(OptionalQueryParamInjectableProvider.class);
        config.addProvider(KVPairsProvider.class);
        if (useBasicAuth) {
            config.addProvider(new BasicAuthProvider<User>(authenticator, "spawn"));
        } else {
            config.addProvider(new SpawnAuthProvider<User>(authenticator, "spawn"));
        }

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
