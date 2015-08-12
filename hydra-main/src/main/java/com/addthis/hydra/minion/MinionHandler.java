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
package com.addthis.hydra.minion;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;

import java.util.Enumeration;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.SimpleExec;

import com.addthis.hydra.job.mq.JobKey;
import com.addthis.maljson.JSONObject;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MinionHandler extends AbstractHandler {
    private static final Logger log = LoggerFactory.getLogger(MinionHandler.class);

    private final Minion minion;

    public MinionHandler(Minion minion) {
        this.minion = minion;
    }

    @Override
    public void doStop() {
    }

    @Override
    public void doStart() {
    }

    @Override
    public void handle(String target,
                       Request request,
                       HttpServletRequest httpServletRequest,
                       HttpServletResponse httpServletResponse) throws IOException, ServletException {
        try {
            doHandle(target, request, httpServletRequest, httpServletResponse);
        } catch (IOException | ServletException io) {
            throw io;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public void doHandle(String target,
                         Request baseRequest,
                         HttpServletRequest request,
                         HttpServletResponse response) throws Exception {
        response.setBufferSize(65535);
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Headers", "accept, username");
        response.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT");
        KVPairs kv = new KVPairs();
        boolean handled = true;
        for (Enumeration<String> e = request.getParameterNames(); e.hasMoreElements(); ) {
            String k = e.nextElement();
            String v = request.getParameter(k);
            kv.add(k, v);
        }
        if (target.equals("/ping")) {
            response.getWriter().write("ACK");
        } else if (target.startsWith("/metrics")) {
            minion.metricsHandler.handle(target, baseRequest, request, response);
        } else if (target.equals("/job.port")) {
            String job = kv.getValue("id");
            int taskID = kv.getIntValue("node", -1);
            JobKey key = new JobKey(job, taskID);
            JobTask task = minion.tasks.get(key.toString());
            Integer jobPort = null;
            if (task != null) {
                jobPort = task.getPort();
            }
            response.getWriter().write("{port:" + (jobPort != null ? jobPort : 0) + "}");
        } else if (target.equals("/job.profile")) {
            String jobName = kv.getValue("id", "") + "/" + kv.getIntValue("node", 0);
            JobTask job = minion.tasks.get(jobName);
            if (job != null) {
                response.getWriter().write(job.profile());
            } else {
                response.sendError(400, "No Job");
            }
        } else if (target.equals("/job.head")) {
            String jobName = kv.getValue("id", "") + "/" + kv.getIntValue("node", 0);
            int lines = kv.getIntValue("lines", 10);
            boolean out = !kv.getValue("out", "0").equals("0");
            boolean err = !kv.getValue("err", "0").equals("0");
            String html = kv.getValue("html");
            JobTask job = minion.tasks.get(jobName);
            if (job != null) {
                String outB = out ? LogUtils.head(job.logOut, lines) : "";
                String errB = err ? LogUtils.head(job.logErr, lines) : "";
                if (html != null) {
                    html = html.replace("{{out}}", outB);
                    html = html.replace("{{err}}", errB);
                    response.setContentType("text/html");
                    response.getWriter().write(html);
                } else {
                    response.getWriter().write(new JSONObject().put("out", outB).put("err", errB).toString());
                }
            } else {
                response.sendError(400, "No Job");
            }
        } else if (target.equals("/job.tail")) {
            String jobName = kv.getValue("id", "") + "/" + kv.getIntValue("node", 0);
            int lines = kv.getIntValue("lines", 10);
            boolean out = !kv.getValue("out", "0").equals("0");
            boolean err = !kv.getValue("err", "0").equals("0");
            String html = kv.getValue("html");
            JobTask job = minion.tasks.get(jobName);
            if (job != null) {
                String outB = out ? LogUtils.tail(job.logOut, lines) : "";
                String errB = err ? LogUtils.tail(job.logErr, lines) : "";
                if (html != null) {
                    html = html.replace("{{out}}", outB);
                    html = html.replace("{{err}}", errB);
                    response.setContentType("text/html");
                    response.getWriter().write(html);
                } else {
                    response.getWriter().write(new JSONObject().put("out", outB).put("err", errB).toString());
                }
            } else {
                response.sendError(400, "No Job");
            }
        } else if (target.equals("/job.log")) {
            String jobName = kv.getValue("id", "") + "/" + kv.getIntValue("node", 0);
            int offset = kv.getIntValue("offset", -1);
            int lines = kv.getIntValue("lines", 10);
            int runsAgo = kv.getIntValue("runsAgo", 0);
            boolean out = kv.getValue("out", "1").equals("1");
            JobTask job = minion.tasks.get(jobName);
            if (job != null) {
                String logSuffix = out ? "out" : "err";
                // treat negative runs as searches for archived task-error logs rather than bothering with extra flag
                if (runsAgo < 0) {
                    runsAgo *= -1;
                    runsAgo -= 1;
                    logSuffix += ".bad";
                }
                response.getWriter().write(LogUtils.readLogLines(job, offset, lines, runsAgo, logSuffix).toString());
            } else {
                response.sendError(400, "No Job");
            }
        } else if (target.equals("/jobs.import")) {
            int count = minion.updateJobsMeta(new File(kv.getValue("dir", ".")));
            response.getWriter().write("imported " + count + " jobs");
        } else if (target.equals("/xdebug/findnextport")) {
            response.getWriter().write("port: " + 0);
        } else if (target.equals("/active.tasks")) {
            Minion.capacityLock.lock();
            try {
                response.getWriter().write("tasks: " + minion.activeTaskKeys.toString());
            } finally {
                Minion.capacityLock.unlock();
            }
        } else if (target.equals("/task.size")) {
            String jobId = kv.getValue("id");
            int taskId = kv.getIntValue("node", -1);
            if (jobId != null && taskId >= 0) {
                String duOutput = new SimpleExec(MacUtils.ducmd + " -s --block-size=1 " + ProcessUtils.getTaskBaseDir(
                        minion.rootDir.getAbsolutePath(), jobId, taskId)).join().stdoutString();
                response.getWriter().write(duOutput.split("\t")[0]);
            }
        } else {
            response.sendError(404);
        }
        ((Request) request).setHandled(handled);
    }
}
