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

import com.addthis.basis.util.Parameter;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.util.resource.Resource;

public class Main {
    public static void main(String[] args) throws Exception {
        int serverPort = Parameter.intValue("mesh.http.port", 6001);
        String meshHost = Parameter.value("mesh.host", "localhost");
        int meshPort = Parameter.intValue("mesh.port", 5000);
        Server server = new Server(serverPort);
        MeshConnection connection = new MeshConnection(meshHost, meshPort);

        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setDirectoriesListed(true);
        resourceHandler.setBaseResource(Resource.newClassPathResource("/meshy-http-webroot"));

        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[] {resourceHandler, new MeshHandler(connection)});

        server.setHandler(handlers);
        server.start();
        server.join();
    }
}
