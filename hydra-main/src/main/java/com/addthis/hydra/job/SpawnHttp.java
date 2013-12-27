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
package com.addthis.hydra.job;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import com.addthis.basis.kv.KVPair;
import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Strings;

import com.addthis.hydra.util.MetricsServletMaker;
import com.addthis.maljson.JSONObject;

import org.apache.commons.lang3.CharEncoding;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.slf4j.Logger;


import org.slf4j.LoggerFactory;
public class SpawnHttp extends AbstractHandler {

    private static Logger log = LoggerFactory.getLogger(SpawnHttp.class);

    private final Spawn spawn;
    private final File webDir;
    private final ConcurrentHashMap<String, JSONObject> auth = new ConcurrentHashMap<String, JSONObject>();
    private final LinkedList<Runnable> onStopList = new LinkedList<Runnable>();
    private final ServletHandler metricsHandler;

    public SpawnHttp(final Spawn spawn, final File webDir) {
        this.webDir = webDir;
        this.spawn = spawn;
        this.metricsHandler = MetricsServletMaker.makeHandler();
    }

    public Spawn spawn() {
        return spawn;
    }

    public void addOnStop(Runnable onStop) {
        onStopList.add(onStop);
    }

    /**
     * simple kvpairs wrapper service
     */
    public abstract class KVService extends HTTPService {

        public abstract void kvCall(KVPairs kv) throws Exception;

        @Override
        public void httpService(HTTPLink link) throws Exception {
            KVPairs kv = link.getRequestValues();
            try {
                kvCall(kv);
                JSONObject ret = new JSONObject();
                for (KVPair p : kv) {
                    ret.put(p.getKey(), p.getValue());
                }
                link.sendShortReply(200, "OK", ret.toString(1));
            } catch (Exception ex) {
                ex.printStackTrace();
                link.sendShortReply(500, "Server Error", new JSONObject().put("error", ex.getMessage()).toString());
            }
        }
    }

    @Override
    public void doStart() {
    }

    @Override
    public void doStop() {
        for (Runnable onStop : onStopList) {
            onStop.run();
        }
    }

    /**
     * hack layer to make Jetty look a litle more like HTTPServer for this migration
     */
    private HashMap<String, HTTPService> serviceMap = new HashMap<String, HTTPService>();

    public void mapService(String path, HTTPService service) {
        serviceMap.put(path, service);
    }

    public abstract static class HTTPService {

        public abstract void httpService(HTTPLink link) throws Exception;

        public static String getCookie(HTTPLink link, String cookieName) {
            if (link.request.getCookies() == null) {
                return null;
            }
            for (Cookie cookie : link.request.getCookies()) {
                if (cookie.getName().equals(cookieName)) {
                    return cookie.getValue();
                }
            }
            return null;
        }

        public static void require(boolean test, String msg) throws Exception {
            if (!test) {
                throw new Exception("test failed with '" + msg + "'");
            }
        }

        public static Long getValidLong(KVPairs kv, String key, Long defaultValue) {
            String l = kv.getValue(key, defaultValue != null ? defaultValue + "" : null);
            if (l == null || l.trim().equals("0") || l.startsWith("-")) {
                return null;
            }
            try {
                return Long.parseLong(l);
            } catch (Exception ex) {
                return null;
            }
        }

        public static HashSet<String> csvListToSet(String list) {
            if (list != null) {
                HashSet<String> set = new HashSet<String>();
                for (String s : Strings.splitArray(list, ",")) {
                    set.add(s);
                }
                return set;
            }
            return null;
        }
    }

    public static class HTTPLink {

        private String target;
        private HttpServletRequest request;
        private HttpServletResponse response;
        private KVPairs params;
        private byte post[];

        HTTPLink(String target, HttpServletRequest request, HttpServletResponse response) {
            this.target = target;
            this.request = request;
            this.response = response;
            if (log.isDebugEnabled()) log.debug("target=" + target + " request=" + request);
        }

        public String target() {
            return target;
        }

        public HttpServletRequest request() {
            return request;
        }

        public HttpServletResponse response() {
            return response;
        }

        public KVPairs params() {
            return getRequestValues();
        }

        public KVPairs getRequestValues() {
            if (params == null) {
                params = new KVPairs();
                for (Enumeration<String> e = request.getParameterNames(); e.hasMoreElements();) {
                    String k = e.nextElement();
                    String v = request.getParameter(k);
                    params.add(k, v);
                }
            }
            return params;
        }

        public void sendShortReply(int code, String topic, String message) {
            try {
                response.setStatus(code);
                response.setHeader("topic", topic);
                response.getWriter().write(message);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public void sendShortReply(int code, String topic, String contentType, String message) {
            try {
                response.setStatus(code);
                response.setHeader("topic", topic);
                response.setHeader("Content-Disposition", contentType);
                response.getWriter().write(message);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public String getPostBody() {
            if (post == null) {
                try {
                    post = Bytes.readFully(request.getInputStream());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return Bytes.toString(post);
        }

        public void setResponseContentType(String contentType) {
            response.setContentType(contentType);

        }
    }

    @Override
    public void handle(String target, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
        //To change body of implemented methods use File | Settings | File Templates.
        httpServletResponse.setCharacterEncoding(CharEncoding.UTF_8);
        if (target.equals("/")) {
            httpServletResponse.sendRedirect("/spawn/index.html");
        } else if (target.startsWith("/metrics")) {
            if (!metricsHandler.isStarted()) {
                log.warn("Metrics servlet failed to start");
            }
            metricsHandler.handle(target, request, httpServletRequest, httpServletResponse);
        } else {
            HTTPService handler = serviceMap.get(target);
            if (handler != null) {
                try {
                    handler.httpService(new HTTPLink(target, request, httpServletResponse));
                } catch (Exception ex) {
                    log.warn("handler error " + ex, ex);
                    httpServletResponse.sendError(500, ex.getMessage());
                }
            } else {
                File file = new File(webDir, target);
                if (file.exists() && file.isFile()) {
                    OutputStream out = httpServletResponse.getOutputStream();
                    InputStream in = new FileInputStream(file);
                    byte buf[] = new byte[1024];
                    int read = 0;
                    while ((read = in.read(buf)) >= 0) {
                        out.write(buf, 0, read);
                    }
                } else {
                    log.warn("[http.unhandled] " + target);
                    httpServletResponse.sendError(404);
                }
            }
        }
        ((Request) request).setHandled(true);
    }

}
