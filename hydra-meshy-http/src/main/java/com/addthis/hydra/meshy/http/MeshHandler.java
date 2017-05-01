package com.addthis.hydra.meshy.http;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import java.util.Collection;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

import com.addthis.meshy.service.file.FileReference;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class MeshHandler extends AbstractHandler {
    MeshConnection connection;
    ObjectMapper mapper;

    public MeshHandler(MeshConnection connection) {
        this.connection = connection;
        this.mapper = new ObjectMapper();
    }

    @Override public void handle(
            String s, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

        switch (request.getPathInfo()) {
            case "/get":
                response.setStatus(HttpServletResponse.SC_OK);
                response.setContentType("text/plain");
                doGet(request.getParameter("uuid"), request.getParameter("path"), response.getOutputStream());
                break;
            case "/list":
                response.setStatus(HttpServletResponse.SC_OK);
                response.setContentType("application/json");
                doList(request.getParameter("path"), response.getOutputStream());
                break;
            default:
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().print("Use /get or /list");
        }
        baseRequest.setHandled(true);
    }

    private void doGet(String uuid, String path, OutputStream outputStream) throws IOException {
        Function<InputStream, ? extends InputStream> inputTransfomer;
        if (path.endsWith(".gz")) {
            inputTransfomer = in -> {
                try {
                    return new GZIPInputStream(in);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
        } else {
            inputTransfomer = Function.identity();
        }
        connection.streamFile(uuid, path, outputStream, inputTransfomer);
        outputStream.close();
    }

    private void doList(String path, OutputStream outputStream) throws IOException {
        Collection<FileReference> files = connection.list(path);
        mapper.writeValue(outputStream, files);
        outputStream.close();
    }
}
