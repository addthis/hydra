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
package com.addthis.hydra.meshy.http;

import com.addthis.codec.config.Configs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.Resource;

public class MeshyHttp {
    private final int serverPort;
    private final int meshPort;
    private final String meshHost;

    public static void main(String[] args) throws Exception {
        MeshyHttp meshyHttp = Configs.newDefault(MeshyHttp.class);
        meshyHttp.start();
    }

    @JsonCreator public MeshyHttp(@JsonProperty(value = "serverPort", required = true) int serverPort,
            @JsonProperty(value = "meshHost", required = true) String meshHost,
            @JsonProperty(value = "meshPort", required = true) int meshPort) {
        this.serverPort = serverPort;
        this.meshHost = meshHost;
        this.meshPort = meshPort;
    }

    public void start() throws Exception {
        Server server = new Server(serverPort);
        MeshConnection connection = new MeshConnection(meshHost, meshPort);

        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setDirectoriesListed(true);
        resourceHandler.setBaseResource(Resource.newClassPathResource("/meshy-http-webroot"));

        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[]{resourceHandler, new MeshHandler(connection)});

        server.setHandler(handlers);
        server.start();
        server.join();
    }
}
