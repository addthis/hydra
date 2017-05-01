package com.addthis.hydra.meshy.http;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.util.resource.Resource;

public class Main {
    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);
        MeshConnection connection = new MeshConnection("spawn-lax", 5500);

        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setDirectoriesListed(true);
//        resourceHandler.setBaseResource(Resource.newClassPathResource("/webroot"));
        resourceHandler.setResourceBase("/Users/ted/workspace/hydra/hydra-meshy-http/src/main/resources/webroot");

        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[] {resourceHandler, new MeshHandler(connection)});

        server.setHandler(handlers);
        server.start();
        server.join();
    }
}
