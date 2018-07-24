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
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;

import java.util.Enumeration;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.SimpleExec;

import com.addthis.hydra.job.mq.JobKey;
import com.addthis.hydra.util.PrometheusServletCreator;
import com.addthis.maljson.JSONObject;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinionServletContext {
    private static final Logger log = LoggerFactory.getLogger(MinionServletContext.class);
    Minion minion;

    public MinionServletContext(Minion minion) {
        this.minion = minion;
    }

    public ServletContextHandler build() {
        ServletContextHandler servletContex = new ServletContextHandler();
        PrometheusServletCreator.create(servletContex);

        servletContex.addServlet(new ServletHolder(new PingServlet()), "/ping");
        servletContex.addServlet(new ServletHolder(new JobPortServlet()), "/job.port");
        servletContex.addServlet(new ServletHolder(new JobProfileServlet()), "/job.profile");
        servletContex.addServlet(new ServletHolder(new JobHeadServlet()), "/job.head");
        servletContex.addServlet(new ServletHolder(new JobTailServlet()), "/job.tail");
        servletContex.addServlet(new ServletHolder(new JobLogServlet()), "/job.log");
        servletContex.addServlet(new ServletHolder(new JobImportServlet()), "/jobs.import");
        servletContex.addServlet(new ServletHolder(new FindExtPortServlet()), "/xdebug/findnextport");
        servletContex.addServlet(new ServletHolder(new ActiveTasksServlet()), "/active.tasks");
        servletContex.addServlet(new ServletHolder(new TaskSizeServlet()), "/task.size");

        return servletContex;
    }

    private static KVPairs parseRequest(HttpServletRequest request) {
        KVPairs kv = new KVPairs();
        for (Enumeration<String> e = request.getParameterNames(); e.hasMoreElements(); ) {
            String k = e.nextElement();
            String v = request.getParameter(k);
            kv.add(k, v);
        }
        return kv;
    }
    
    private static class PingServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            response.getWriter().write("ACK");
        }
    }

    private class JobPortServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            KVPairs kv = parseRequest(request);

            String job = kv.getValue("id");
            int taskID = kv.getIntValue("node", -1);
            JobKey key = new JobKey(job, taskID);
            JobTask task = minion.tasks.get(key.toString());
            Integer jobPort = null;
            if (task != null) {
                jobPort = task.getPort();
            }
            response.getWriter().write("{port:" + (jobPort != null ? jobPort : 0) + "}");
        }
    }

    private class JobProfileServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            KVPairs kv = parseRequest(request);
            String jobName = kv.getValue("id", "") + "/" + kv.getIntValue("node", 0);
            JobTask job = minion.tasks.get(jobName);
            if (job != null) {
                response.getWriter().write(job.profile());
            } else {
                response.sendError(400, "No Job");
            }
        }
    }

    private class JobHeadServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            KVPairs kv = parseRequest(request);
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
                    try {
                        response.getWriter().write(new JSONObject().put("out", outB).put("err", errB).toString());
                    } catch (Exception e) {
                        log.error("Failed to generate response: ", e);
                    }
                }
            } else {
                response.sendError(400, "No Job");
            }
        }
    }

    private class JobTailServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            KVPairs kv = parseRequest(request);
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
                    try {
                        response.getWriter().write(new JSONObject().put("out", outB).put("err", errB).toString());
                    } catch (Exception e) {
                        log.error("Failed to generate response: ", e);
                    }
                }
            } else {
                response.sendError(400, "No Job");
            }
        }
    }

    private class JobLogServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            KVPairs kv = parseRequest(request);
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
                try {
                    response.getWriter()
                            .write(LogUtils.readLogLines(job, offset, lines, runsAgo, logSuffix).toString());
                } catch (Exception e) {
                    log.error("Failed to generate response: ", e);
                }
            } else {
                response.sendError(400, "No Job");
            }
        }
    }

    private class JobImportServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            KVPairs kv = parseRequest(request);
            int count = minion.updateJobsMeta(new File(kv.getValue("dir", ".")));
            response.getWriter().write("imported " + count + " jobs");
        }
    }

    private static class FindExtPortServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            response.getWriter().write("port: " + 0);
        }
    }

    private class ActiveTasksServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            Minion.capacityLock.lock();
            try {
                response.getWriter().write("tasks: " + minion.activeTaskKeys.toString());
            } finally {
                Minion.capacityLock.unlock();
            }
        }
    }

    private class TaskSizeServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            KVPairs kv = parseRequest(request);
            String jobId = kv.getValue("id");
            int taskId = kv.getIntValue("node", -1);
            if (jobId != null && taskId >= 0) {
                try {
                    String duOutput =
                            new SimpleExec(MacUtils.ducmd + " -s --block-size=1 " + ProcessUtils.getTaskBaseDir(
                                    minion.rootDir.getAbsolutePath(), jobId, taskId)).join().stdoutString();
                    response.getWriter().write(duOutput.split("\t")[0]);
                } catch (Exception e) {
                    log.error("Failed to generate response: ", e);
                }
            }
        }
    }

}
