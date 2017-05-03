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
