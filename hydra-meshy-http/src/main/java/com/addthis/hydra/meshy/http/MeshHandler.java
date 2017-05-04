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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeshHandler extends AbstractHandler {
    Logger log = LoggerFactory.getLogger(MeshHandler.class);
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
            case "/check-binary":
                response.setStatus(HttpServletResponse.SC_OK);
                response.setContentType("application/json");
                doCheckBinary(request.getParameter("uuid"), request.getParameter("path"), response.getOutputStream());
            default:
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().print("Use /get, /list, or /check-binary");
        }
        baseRequest.setHandled(true);
    }

    private void doGet(String uuid, String path, OutputStream outputStream) {
        try (OutputStream o = outputStream) {
            connection.streamFile(uuid, path, o, MeshHandler.getTransformer(path));
        } catch (IOException e) {
            log.error("Error while streaming file", e);
        }
    }

    private void doList(String path, OutputStream outputStream) {
        Collection<FileReference> files = connection.list(path);
        try (OutputStream o = outputStream) {
            mapper.writeValue(o, files);
        } catch (IOException e) {
            log.error("Failed to write file list to http output", e);
        }
    }

    private void doCheckBinary(String uuid, String path, OutputStream outputStream) {
        try (OutputStream o = outputStream) {
            boolean binary = connection.mightBeBinaryFile(uuid, path, MeshHandler.getTransformer(path));
            // wrap and write the value
            mapper.writeValue(outputStream, new Binary(binary));
        } catch (IOException e) {
            log.error("Error writing binary status to http output", e);
        }
    }

    private static Function<InputStream, ? extends InputStream> getTransformer(String path) {
        if (path.endsWith(".gz")) {
            return in -> {
                try {
                    return new GZIPInputStream(in);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
        }
        return Function.identity();
    }

    private static class Binary {
        public boolean binary;

        public Binary(boolean binary) {
            this.binary = binary;
        }
    }
}
