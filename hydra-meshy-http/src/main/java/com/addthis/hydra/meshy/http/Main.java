package com.addthis.hydra.meshy.http;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.util.resource.Resource;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Required arguments: <server-port> <mesh-host> <mesh-port>");
        }

        int serverPort = Integer.parseInt(args[0]);
        String meshHost = args[1];
        int meshPort = Integer.parseInt(args[2]);
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
