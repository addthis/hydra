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
package com.addthis.hydra.job.web.old;

import java.io.InputStream;
import java.io.StringWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.kv.KVPair;
import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.net.HttpUtil;
import com.addthis.basis.net.http.HttpResponse;
import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessStrings;
import com.addthis.basis.util.TokenReplacerOverflowException;

import com.addthis.bark.StringSerializer;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobExpand;
import com.addthis.hydra.job.JobParameter;
import com.addthis.hydra.job.JobQueryConfig;
import com.addthis.hydra.job.JobState;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.RebalanceOutcome;
import com.addthis.hydra.job.alias.AliasManager;
import com.addthis.hydra.job.entity.JobCommand;
import com.addthis.hydra.job.entity.JobEntityManager;
import com.addthis.hydra.job.entity.JobMacro;
import com.addthis.hydra.minion.Minion;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.ClientEvent;
import com.addthis.hydra.job.spawn.ClientEventListener;
import com.addthis.hydra.job.spawn.DeleteStatus;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.spawn.SystemManager;
import com.addthis.hydra.job.store.DataStoreUtil;
import com.addthis.hydra.job.web.JobRequestHandler;
import com.addthis.hydra.job.web.JobRequestHandlerImpl;
import com.addthis.hydra.job.web.KVUtils;
import com.addthis.hydra.job.web.old.SpawnHttp.HTTPLink;
import com.addthis.hydra.job.web.old.SpawnHttp.HTTPService;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;
import com.addthis.meshy.service.file.FileReference;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.job.web.KVUtils.getValueOpt;


public class SpawnManager {

    private static final Logger log = LoggerFactory.getLogger(SpawnManager.class);
    private static final int batchInterval = Integer.parseInt(System.getProperty("spawn.batchtime", "500"));
    private static final int pollTimeout = Integer.parseInt(System.getProperty("spawn.polltime", "1000"));
    private static final String defaultUser = "UNKNOWN_USER";

    public void register(final SpawnHttp server) {
        final Spawn spawn = server.spawn();
        final AliasManager aliasManager = spawn.getAliasManager();
        final JobEntityManager<JobMacro> jobMacroManager = spawn.getJobMacroManager();
        final JobEntityManager<JobCommand> jobCommandManager = spawn.getJobCommandManager();
        final JobRequestHandler jobRequestHandler = new JobRequestHandlerImpl(spawn);
        final SystemManager systemManager = spawn.getSystemManager();
        /** url called via ajax to listen for change events */
        server.mapService("/listen", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) {
                KVPairs kv = link.getRequestValues();
                long timeout = kv.getLongValue("timeout", pollTimeout);
                ClientEventListener listener = spawn.getClientEventListener(kv.getValue("clientID", "noid"));
                try {
                    ClientEvent nextEvent = listener.events.poll(timeout, TimeUnit.MILLISECONDS);
                    if (nextEvent != null) {
                        link.sendJSON(200, nextEvent.topic(), nextEvent.message());
                    } else {
                        link.sendJSON(200, "queue.empty", json());
                    }
                } catch (InterruptedException ex) {
                    link.sendJSON(200, "queue.empty", json());
                } catch (Exception ex)  {
                    log.warn("", ex);
                    link.sendShortReply(500, "Internal Error", ex.getMessage());
                }
            }
        });
        /** url called via ajax to listen for batch change events */
        server.mapService("/listen.batch", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) {
                KVPairs kv = link.getRequestValues();
                long timeout = kv.getLongValue("timeout", pollTimeout);
                long batchTime = kv.getLongValue("batchtime", batchInterval);
                ClientEventListener listener = spawn.getClientEventListener(kv.getValue("clientID", "noid"));
                try {
                    ClientEvent nextEvent = listener.events.poll(timeout, TimeUnit.MILLISECONDS);
                    if (nextEvent != null) {
                        long mark = System.currentTimeMillis();
                        JSONArray payload = new JSONArray();
                        payload.put(nextEvent.toJSON());
                        for (int i = 50; i > 0; i--) {
                            nextEvent = listener.events.poll(batchTime, TimeUnit.MILLISECONDS);
                            if (nextEvent != null) {
                                payload.put(nextEvent.toJSON());
                            }
                            if (System.currentTimeMillis() - mark > batchTime) {
                                break;
                            }
                        }
                        link.sendJSON(200, "event.batch", payload);
                    } else {
                        link.sendJSON(200, "queue.empty", json());
                    }
                } catch (InterruptedException ex) {
                    link.sendJSON(200, "queue.empty", json());
                } catch (Exception ex)  {
                    log.warn("", ex);
                    link.sendShortReply(500, "Internal Error", ex.getMessage());
                }
            }
        });
        /** url called via ajax to bulk enable/disable jobs */
        server.mapService("/jobs.enable", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String jobarg = kv.getValue("jobs");
                boolean enable = kv.getValue("enable", "1").equals("1");
                if (jobarg != null) {
                    String[] joblist = LessStrings.splitArray(jobarg, ",");
                    emitLogLineForAction(kv, (enable ? "enable" : "disable") + " jobs " + jobarg);
                    for (String jobid : joblist) {
                        IJob job = spawn.getJob(jobid);
                        if (job != null && job.setEnabled(enable)) {
                            spawn.updateJob(job);
                        }
                    }
                    link.sendJSON(200, "OK", json("success",true));
                } else {
                    link.sendJSON(200, "Error", json("error","missing jobs parameter"));
                }
            }
        });
        /** url called via ajax by client to get/set spawn config */
        server.mapService("/setup", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                systemManager.updateDebug(getValueOpt(kv, "debug"));
                systemManager.updateQueryHost(getValueOpt(kv, "queryHost"));
                systemManager.updateSpawnHost(getValueOpt(kv, "spawnHost"));
                systemManager.updateDisabled(getValueOpt(kv, "disabled"));
                getValueOpt(kv, "quiesce").ifPresent(v -> {
                    boolean q = "1".equals(v);
                    if (q != systemManager.isQuiesced()) {
                        String logVerb = q ? "quiesce" : "unquiesce";
                        emitLogLineForAction(kv, logVerb + " the cluster");
                        systemManager.quiesceCluster(q, kv.getValue("user", "anonymous"));
                    }
                });
                JSONObject ret = CodecJSON.encodeJSON(systemManager.getSettings());
                if (kv.getValue("all", "0").equals("1")) {
                    JSONObject macrolist = new JSONObject();
                    JSONObject commandlist = new JSONObject();
                    JSONArray joblist = new JSONArray();
                    JSONArray hostlist = new JSONArray();
                    for (String key : jobMacroManager.getKeys()) {
                        macrolist.put(key, jobMacroManager.getEntity(key).toJSON().put("macro", ""));
                    }
                    for (String key : jobCommandManager.getKeys()) {
                        commandlist.put(key, jobCommandManager.getEntity(key).toJSON());
                    }
                    for (HostState host : spawn.hostManager.listHostStatus(null)) {
                        hostlist.put(spawn.getHostStateUpdateEvent(host));
                    }
                    HashSet<String> ids = csvListToSet(link.getRequestValues().getValue("id"));
                    for (IJob job : spawn.listJobsConcurrentImmutable()) {
                        if (ids == null || ids.contains(job.getId())) {
                            JSONObject jobUpdateEvent = Spawn.getJobUpdateEvent(job);
                            joblist.put(jobUpdateEvent);
                        }
                    }
                    ret.put("macros", macrolist);
                    ret.put("commands", commandlist);
                    ret.put("jobs", joblist);
                    ret.put("hosts", hostlist);
                    ret.put("spawnqueuesize", spawn.getTaskQueuedCount());
                }
                link.sendJSON(200, "OK", ret);
            }
        });
        /** ajax call to minion - for log watching */
        server.mapService("/proxy", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String url = kv.getValue("url");
                String type = kv.getValue("type", "x-www-form-urlencoded");
                String post = kv.getValue("post", "");
                int timeout = kv.getIntValue("timeout", 10000);
                byte[] res = null;
                HttpResponse response = HttpUtil.execute(HttpUtil.makePost(url, type, LessBytes.toBytes(post)), timeout);
                if (response.getStatus() == 200) {
                    res = response.getBody();
                }
                if (res != null && res.length > 0) {
                    link.sendShortReply(200, "OK", LessBytes.toString(res));
                } else {
                    link.sendShortReply(500, "No Content", "");
                }
            }
        });
        server.mapService("/macro.list", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                JSONObject list = new JSONObject();
                for (String key : jobMacroManager.getKeys()) {
                    JobMacro macro = jobMacroManager.getEntity(key);
                    list.put(key, macro.toJSON());
                }
                link.sendJSON(200, "OK", list);
            }
        });
        server.mapService("/macro.get", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                String label = link.getRequestValues().getValue("label", "");
                JobMacro macro = jobMacroManager.getEntity(label);
                if (macro != null) {
                    link.sendJSON(200, "OK", macro.toJSON());
                } else {
                    link.sendJSON(200, "Error", json("error","no such macro"));
                }
            }
        });
        server.mapService("/macro.put", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String label = kv.getValue("label");
                require(label != null, "missing label");
                JobMacro oldMacro = jobMacroManager.getEntity(label);
                String description = kv.getValue("description", oldMacro != null ? oldMacro.getDescription() : null);
                String owner = kv.getValue("owner", oldMacro != null ? oldMacro.getOwner() : null);
                String macro = kv.getValue("macro", oldMacro != null ? oldMacro.getMacro() : null);
                require(description != null, "missing description");
                require(owner != null, "missing owner");
                require(macro != null, "missing macro");
                jobMacroManager.putEntity(label, new JobMacro(owner, description, macro), true);
                link.sendJSON(200, "OK", json("success",true));
            }
        });
        server.mapService("/macro.delete", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                if (jobMacroManager.deleteEntity(link.getRequestValues().getValue("label", ""))) {
                    link.sendJSON(200, "OK", json("success",true));
                } else {
                    link.sendJSON(200, "Error", json("error","macro delete failed"));
                }
            }
        });
        /** url called via ajax to get list of active commands */
        server.mapService("/command.list", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                JSONObject list = new JSONObject();
                for (String key : jobCommandManager.getKeys()) {
                    list.put(key, jobCommandManager.getEntity(key).toJSON());
                }
                link.sendJSON(200, "OK", list);
            }
        });
        /** url called via ajax to add or update a command */
        server.mapService("/command.put", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                String label = kv.getValue("label");
                String command = kv.getValue("command", "").trim();
                String owner = kv.getValue("owner", "unknown").trim();
                require(label != null, "missing label");
                require(command.length() > 0, "missing command");
                String[] cmdtok = LessStrings.splitArray(command, ",");
                for (int i = 0; i < cmdtok.length; i++) {
                    cmdtok[i] = LessBytes.urldecode(cmdtok[i]);
                }
                jobCommandManager.putEntity(label, new JobCommand(owner, cmdtok, kv.getIntValue("cpu", 0), kv.getIntValue("mem", 0), kv.getIntValue("io", 0)), true);
                kv.putValue("return", 1);
            }
        });
        /** url called via ajax to add or update a command */
        server.mapService("/command.delete", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                String label = kv.getValue("label");
                require(label != null, "missing label");
                if (!jobCommandManager.deleteEntity(label)) {
                    throw new Exception("command delete failed");
                }
            }
        });
        /** force refresh of host status -- TODO legacy */
        server.mapService("/hosts.update", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                spawn.requestHostsUpdate();
            }
        });
        /** url called via ajax by client get list of monitored hosts */
        server.mapService("/host.list", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                HashSet<String> ids = csvListToSet(link.getRequestValues().getValue("id"));
                JSONArray list = new JSONArray();
                for (HostState host : spawn.hostManager.listHostStatus(null)) {
                    if (ids == null || ids.contains(host.getHost()) || ids.contains(host.getHostUuid())) {
                        list.put(spawn.getHostStateUpdateEvent(host));
                    }
                }
                link.sendJSON(200, "OK", list);
            }
        });
        /** lists avialable hosts */
        server.mapService("/available.host.list", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                HashSet<String> ids = csvListToSet(link.getRequestValues().getValue("id"));
                JSONArray list = new JSONArray();
                for (String host : spawn.listAvailableHostIds()) {
                    if (ids == null || ids.contains(host)) {
                        list.put(host);
                    }
                }
                link.sendJSON(200, "OK", list);
            }
        });


        /** fail out a host and cause jobs/tasks/replicas to rebalance */
        server.mapService("/host.fail", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                String uuids = kv.getValue("uuids");
                boolean deadFileSystem = kv.getIntValue("deadFs", 1) == 1;
                emitLogLineForAction(kv, "fail host on " + uuids + " deadFileSystem=" + deadFileSystem);
                spawn.markHostsForFailure(uuids, deadFileSystem);
                kv.putValue("return", 1);
            }
        });
        /** cancel the queued failure for a host */
        server.mapService("/cancel.host.fail", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                String uuids = kv.getValue("uuids");
                spawn.unmarkHostsForFailure(uuids);
                kv.putValue("return", 1);
            }
        });
        /** get information regarding the implications of failing a host */
        server.mapService("/host.fail.info", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                String uuids = link.getRequestValues().getValue("uuids");
                boolean deadFs = link.getRequestValues().getIntValue("deadFs", 1) == 1;
                link.sendJSON(200, "OK", spawn.getHostFailWorker().getInfoForHostFailure(uuids, deadFs));
            }
        });
        /** force refresh of host status */
        server.mapService("/hosts.refresh", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                spawn.requestHostsUpdate();
            }
        });
        /** drop host from current monitored list */
        server.mapService("/host.delete", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                String uuid = kv.getValue("uuid");
                emitLogLineForAction(kv, "delete host on " + uuid);
                spawn.deleteHost(kv.getValue("uuid", ""));
            }
        });
        /** url called via ajax by client to rebalance a job */
        server.mapService("/job.rebalance", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String id = kv.getValue("id", "");
                int tasksToMove = kv.getIntValue("tasksToMove", -1);
                emitLogLineForAction(kv, "job rebalance on " + id + " tasksToMove=" + tasksToMove);
                RebalanceOutcome outcome = spawn.rebalanceJob(id, tasksToMove);
                link.sendShortReply(200, "OK", outcome.toString());
            }
        });
        /** push or pull tasks to rebalance a host */
        server.mapService("/host.rebalance", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String id = kv.getValue("uuid", "");
                emitLogLineForAction(kv, "host rebalance on " + id);
                RebalanceOutcome outcome = spawn.rebalanceHost(id);
                link.sendShortReply(200, "OK", outcome.toString());
            }
        });
        /** expand the job's macros and send the text to the user */
        server.mapService("/job.expand", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                String id = link.getRequestValues().getValue("id", "");
                if ("".equals(id)) {
                    link.sendShortReply(404, "Error", "{'error':'invalid job id'}");
                } else {
                    String expandedJobConfig = spawn.expandJob(id);
                    link.sendShortReply(200, "expanded_job", "attachment; filename=expanded_job.json", expandedJobConfig);
                }
            }
        });
        /** url called via ajax by client to rebalance a job */
        server.mapService("/job.synchronize", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String id = kv.getValue("id", "");
                emitLogLineForAction(kv, "job synchronize on " + id);
                if (spawn.synchronizeJobState(id)) {
                    link.sendJSON(200, "OK", json().put("id",id).put("action","synchronized"));
                } else {
                    log.warn("[job.synchronize] " + id + " unable to synchronize job");
                    link.sendJSON(200, "Error", json("error","unable to synchronize job. see spawn log file for details"));
                }
            }
        });
        /** url called via ajax by client to kill and remove a job */
        server.mapService("/job.delete", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String id = kv.getValue("id", "");
                Job job = spawn.getJob(id);
                if (job != null && !job.getState().equals(JobState.IDLE)) {
                    link.sendJSON(500, "ERROR", json("error","A non-IDLE job cannot be deleted"));
                } else {
                    emitLogLineForAction(kv, "job delete on " + id);
                    DeleteStatus status = spawn.deleteJob(id);
                    switch (status) {
                        case SUCCESS:
                            link.sendJSON(200, "OK", json("id",id).put("action","deleted"));
                            break;
                        case JOB_MISSING:
                            log.warn("[job.delete] " + id + " missing job");
                            link.sendJSON(200, "Error", json("error","invalid job id"));
                            break;
                        case JOB_DO_NOT_DELETE:
                            log.warn("[job.delete] " + id + " do not delete parameter");
                            link.sendJSON(200, "Error", json("error","job deletion disabled"));
                            break;
                    }
                }
            }
        });
        /** url called via ajax by client to kill and remove a job */
        server.mapService("/job.deleteTask", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String id = kv.getValue("id", "");
                String host = kv.getValue("host", "");
                Integer node = kv.getIntValue("node", -1);
                boolean isReplica = kv.getValue("replica", "false").equals("true");
                Job job = spawn.getJob(id);

                if (job != null && !job.getState().equals(JobState.IDLE)) {
                    link.sendJSON(200, "Error", json("error","unable to delete non-idle job"));
                } else {
                    emitLogLineForAction(kv, "job delete on " + id);
                    if (spawn.deleteTask(id, host, node, isReplica)) {
                        link.sendJSON(200, "OK", json("id",id+"/"+node).put("action","deleted"));
                    } else {
                        log.warn("[job.deleteTask] " + id + " missing job");
                        link.sendJSON(200, "Error", json("error","invalid job id"));
                    }
                }
            }
        });
        /** url called via ajax by client to stop or kill a job depending on the parameters */
        server.mapService("/job.stop", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String id = kv.getValue("id", "");
                IJob job = spawn.getJob(id);
                if (job == null) {
                    link.sendJSON(200, "Error", json("error","invalid job id"));
                    return;
                }
                boolean cancelRekick = kv.getValue("cancel", "0").equals("1");
                boolean force = kv.getValue("force", "0").equals("1");
                int nodeid = kv.getIntValue("node", -1);
                String logVerb = force ? "kill" : "stop";
                emitLogLineForAction(kv, "job " + logVerb + " on " + id);
                // cancel re-spawning
                if (cancelRekick) {
                    job.setRekickTimeout(null);
                }
                log.warn("[job.stop] " + job.getId() + "/" + nodeid + ", cancel=" + cancelRekick + ", force=" + force);
                // broadcast to all hosts if no node specified
                if (nodeid < 0) {
                    if (force) {
                        spawn.killJob(id);
                    } else {
                        spawn.stopJob(id);
                    }
                } else {
                    if (force) {
                        spawn.killTask(id, nodeid);
                    } else {
                        spawn.stopTask(id, nodeid);
                    }
                }
                link.sendJSON(200, "OK", json("id",job.getId()).put("action","stopped"));
            }
        });
        /**
         * url called via ajax by client to rollback one or more nodes to a previous snapshot
         */
        server.mapService("/job.revert", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String id = kv.getValue("id", "");
                String type = kv.getValue("type", "gold"); // The type of backup to revert to -- defaults to gold.
                int rev = kv.getIntValue("rev", 0); // The # of revisions to go back -- defaults to the last one.
                long time = kv.getLongValue("time", -1); // The time of backup to go back to-- defaults to -1 which will not be used.
                emitLogLineForAction(kv, "job revert on " + id + " of type " + type);
                IJob job = spawn.getJob(id);
                int nodeid = kv.getIntValue("node", -1);
                // broadcast to all hosts if no node specified
                spawn.revertJobOrTask(job.getId(), nodeid, type, rev, time);
                link.sendJSON(200, "OK", json("id",job.getId()).put("action","reverted"));
            }
        });
        server.mapService("/task.swap", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String id = kv.getValue("id", "");
                int nodeid = kv.getIntValue("node", -1);
                String target = kv.getValue("target", spawn.getTask(id, nodeid).getReplicas().get(0).getHostUUID());
                boolean kickOnComplete = kv.getValue("kick", "0").equals("1");
                emitLogLineForAction(kv, "job swap on " + id + " to " + target);
                boolean success = spawn.swapTask(spawn.getTask(id, nodeid), target, kickOnComplete);
                String jobKey = id + "/" + nodeid;
                String message = success ? "performed swap on " + jobKey : "couldn't swap " + jobKey + "; check spawn log for details";
                link.sendShortReply(200, "OK", message);
            }
        });
        server.mapService("/task.queue.list", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                link.sendJSON(200, "OK", spawn.getTaskQueueAsJSONArray());
            }
        });
        server.mapService("/task.queue.size", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                link.sendJSON(200, "OK", json().put("size", spawn.getTaskQueuedCount()));
            }
        });
        server.mapService("/job.list", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                HashSet<String> ids = csvListToSet(kv.getValue("id"));
                String owner = kv.getValue("owner");
                JSONArray a = new JSONArray();
                for (IJob job : spawn.listJobsConcurrentImmutable()) {
                    if ((owner == null && ids == null) || (ids != null && ids.contains(job.getId())) || LessStrings.isEqual(
                            owner, job.getOwner())) {
                        a.put(job.toJSON().put("config", ""));
                    }
                }
                link.sendJSON(200, "OK", a);
            }
        });
        server.mapService("/job.get", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String jobId = kv.getValue("id", "");
                IJob job = spawn.getJob(jobId);
                if (job != null) {
                    JSONObject jobobj = job.toJSON();
                    jobobj.put("config", spawn.getJobConfig(jobId));
                    String field = kv.getValue("field");
                    if (field != null) {
                        Object fieldObject = jobobj.get(field);
                        if (fieldObject != null) {
                            link.sendShortReply(200, "OK", fieldObject.toString());
                            return;
                        }
                    }
                    link.sendJSON(200, "OK", jobobj);
                } else {
                    link.sendJSON(200, "Error", json("error","invalid job id"));
                }
            }
        });
        server.mapService("/jobdirs.check", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String id = kv.getValue("id", "");
                int node = kv.getIntValue("node", -1);
                IJob job = spawn.getJob(id);
                if (job != null) {
                    link.sendJSON(200, "OK", json("result", spawn.checkTaskDirText(id, node)));
                } else {
                    link.sendJSON(200, "Error", json("error","invalid job id"));
                }
            }
        });
        server.mapService("/jobdirs.fix", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String id = kv.getValue("id", "");
                int node = kv.getIntValue("node", -1);
                IJob job = spawn.getJob(id);
                if (job != null) {
                    link.sendJSON(200, "OK", json("result", spawn.fixTaskDir(id, node, false, false).toString()));
                } else {
                    link.sendJSON(200, "Error", json("error","invalid job id"));
                }
            }
        });
        // todo: This endpoint appears unused.
        // It is called by the push_conf script
        server.mapService("/job.set", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) {
                try {
                    jobset(link);
                } catch (Exception ex) {
                    link.sendJSON(500, "Error", json("error",ex.getMessage()));
                    log.trace("500 Error", ex);
                }
            }

            private void jobset(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                String id = kv.getValue("id", "");
                IJob job = spawn.getJob(id);
                if (job != null) {
                    job = new Job(job);
                    String field = kv.getValue("field");
                    if (field != null) {
                        JSONObject jobobj = job.toJSON();
                        Object oldvalue = jobobj.opt(field);
                        String value = kv.getValue("value");
                        if (value == null) {
                            jobobj.remove(field);
                        } else if (oldvalue != null) {
                            Class<?> oldclass = oldvalue.getClass();
                            if (oldclass == Integer.class) {
                                jobobj.put(field, Integer.parseInt(value));
                            } else if (oldclass == Long.class) {
                                jobobj.put(field, Long.parseLong(value));
                            } else if (oldclass == Double.class) {
                                jobobj.put(field, Double.parseDouble(value));
                            } else if (oldclass == Boolean.class) {
                                jobobj.put(field, Boolean.parseBoolean(value));
                            } else if (oldclass == JSONObject.class) {
                                jobobj.put(field, new JSONObject(value));
                            } else if (oldclass == JSONArray.class) {
                                jobobj.put(field, new JSONArray(value));
                            } else {
                                jobobj.put(field, value);
                            }
                        } else {
                            jobobj.put(field, value);
                        }
                        CodecJSON.decodeString(job, jobobj.toString());
                        spawn.updateJob(job);
                        spawn.submitConfigUpdate(id, null);
                    } else {
                        updateJobFromCall(link, spawn);
                    }
                    link.sendJSON(200, "OK", job.toJSON());
                } else {
                    link.sendJSON(200, "Error", json("error","invalid job id"));
                }
            }
        });
        /** url for submitting new jobs or respawning existing ones */
        server.mapService("/job.submit", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) {
                KVPairs kv = link.getRequestValues();
                log.debug("job.submit --> {}", kv.toString());
                try {
                    if (kv.count() > 0) {
                        String username = kv.getValue("user", "anonymous");
                        Job job = jobRequestHandler.createOrUpdateJob(kv, username);
                        // optionally kicks the job/task
                        jobRequestHandler.maybeKickJobOrTask(kv, job);
                    }
                    link.sendJSON(200, "OK", json("updated",true));
                } catch (Exception ex) {
                    link.sendJSON(500, "Error", json("error",ex.getMessage()));
                    log.trace("[job.submit] Error", ex);
                }
            }
        });
        /** url for updating job meta-data w/out kicking/rekicking it */
        server.mapService("/job.meta", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                updateJobFromCall(link, spawn);
                link.sendJSON(200, "OK", json("updated",true));
            }
        });
        /** url for creating a job */
        server.mapService("/job.create", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                IJob job = spawn.createJob(
                        kv.getValue("owner", "anonymous"),
                        kv.getIntValue("nodes", -1),
                        Arrays.asList(LessStrings.splitArray(kv.getValue("hosts", ""), ",")),
                        kv.getValue("minionType", Minion.defaultMinionType),
                        kv.getValue("command"));
                String id = job.getId();
                kv.addValue("id", id);
                emitLogLineForAction(kv, "create job " + id);
                updateJobFromCall(kv, spawn);
            }
        });
        /** url for starting a job */
        server.mapService("/job.start", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                String id = kv.getValue("job", "");
                emitLogLineForAction(kv, "start job " + id);
                spawn.startJob(id, 1);
            }
        });
        /** url for killing a job */
        server.mapService("/job.kill", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                String id = kv.getValue("job", "");
                emitLogLineForAction(kv, "kill job " + id);
                spawn.killJob(id);
            }
        });
        /** Automatically balance tasks between hosts */
        server.mapService("/autobalance", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                spawn.autobalance();
            }
        });

        server.mapService("/task.move", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                String id = kv.getValue("job", "");
                int node = kv.getIntValue("node", 0);
                boolean isReplica = kv.getValue("rep", "0").equals("1");
                JobTask task = spawn.getTask(id, node);
                String defaultSource = isReplica ? task.getReplicas().get(0).getHostUUID() : task.getHostUUID();
                String source = kv.getValue("source", defaultSource);
                String target = kv.getValue("target", "");
                if (spawn.moveTask(task.getJobKey(), source, target)) {
                    emitLogLineForAction(kv, "move job " + id + " node " + node + " from " + source + " to " + target);
                }
            }
        });

        /** url for starting a task */
        server.mapService("/task.start", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                String job = kv.getValue("job", "");
                int task = kv.getIntValue("task", -1);
                emitLogLineForAction(kv, "start task " + job + "/" + task);
                spawn.startTask(job, task, true, 1, false);
            }
        });
        /** url for stopping a task */
        server.mapService("/task.stop", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                String job = kv.getValue("job", "");
                int task = kv.getIntValue("task", -1);
                emitLogLineForAction(kv, "stop task " + job + "/" + task);
                spawn.stopTask(job, task);
            }
        });
        /** url for killing a task */
        server.mapService("/task.kill", new KVService() {
            @Override
            public void kvCall(KVPairs kv) throws Exception {
                String job = kv.getValue("job", "");
                int task = kv.getIntValue("task", -1);
                emitLogLineForAction(kv, "kill task " + job + "/" + task);
                spawn.killTask(job, task);
            }
        });
        server.mapService("/alias.list", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, List<String>> aliases = aliasManager.getAliases();
                    StringWriter sw = new StringWriter();
                    mapper.writeValue(sw, aliases);
                    link.sendShortReply(200, "OK", sw.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                    link.sendJSON(500, "Error", json("error",e.getMessage()));
                }
            }
        });
        server.mapService("/alias.put", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                if (!kv.hasKey("alias") || !kv.hasKey("jobs")) {
                    // fix code
                    link.sendJSON(200, "Error", json("error","missing alias or job list"));
                    return;
                }
                try {
                    List<String> jobs = Lists.newArrayList(Splitter.on(',').split(kv.getValue("jobs")));
                    aliasManager.addAlias(kv.getValue("alias"), jobs);
                    link.sendJSON(200, "OK", json("success",true));
                } catch (Exception e) {
                    link.sendJSON(500, "Error", json("error",e.getMessage()));
                    log.trace("500 Error", e);
                }
            }
        });
        server.mapService("/alias.delete", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                if (!kv.hasKey("alias")) {
                    link.sendJSON(500, "Error", json("error","missing alias"));
                    return;
                }
                try {
                    aliasManager.deleteAlias(kv.getValue("alias"));
                    link.sendJSON(200, "OK", json("success",true));
                } catch (Exception e) {
                    link.sendJSON(500, "Error", json("error",e.getMessage()));
                    log.trace("500 Error", e);
                }
            }
        });
        server.mapService("/zk.ls", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                try {
                    List<String> list = spawn.getZkClient().getChildren().forPath(kv.getValue("path", "/"));
                    JSONArray arr = new JSONArray();
                    for (String i : list) {
                        arr.put(i);
                    }
                    link.sendJSON(200, "OK", arr);
                } catch (Exception e) {
                    link.sendJSON(500, "Error", json("error",e.getMessage()));
                    log.trace("500 Error", e);
                }
            }
        });
        server.mapService("/zk.get", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                try {
                    String o = StringSerializer.deserialize(spawn.getZkClient().getData().forPath(kv.getValue("path", "/")));
                    String reply = CodecJSON.encodeString(o);
                    link.sendShortReply(200, "OK", reply != null && reply.length() > 0 ? reply : "");
                } catch (Exception e) {
                    link.sendJSON(500, "Error", json("error",e.getMessage()));
                }
            }
        });
        server.mapService("/mesh.ls", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                try {
                    Collection<FileReference> list = spawn.getMeshyClient().listFiles(new String[]{kv.getValue("path", "/*")});
                    JSONArray arr = new JSONArray();
                    for (FileReference file : list) {
                        arr.put(new JSONObject().put("uuid", file.getHostUUID()).put("name", file.name).put("size", file.size).put("date", file.lastModified));
                    }
                    link.sendJSON(200, "OK", arr);
                } catch (Exception e) {
                    link.sendJSON(500, "Error", json("error",e.getMessage()));
                }
            }
        });
        server.mapService("/mesh.get", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                try {
                    InputStream in = spawn.getMeshyClient().readFile(kv.getValue("uuid", "-"), kv.getValue("path", "-"));
                    byte[] data = LessBytes.readFully(in);
                    String value = LessBytes.toString(data);
                    link.sendShortReply(200, "OK", value.length() > 0 ? value : "");
                } catch (Exception e) {
                    link.sendShortReply(200, "No Content", "");
                }
            }
        });

        server.mapService("/host.enable", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                emitLogLineForAction(kv, "enable host");
                String hosts = kv.getValue("hosts");
                try {
                    spawn.toggleHosts(hosts, false);
                    link.sendJSON(200, "OK", json("success",true));
                } catch (Exception e) {
                    link.sendJSON(500, "Error", json("error",e.getMessage()));
                }
            }
        });
        server.mapService("/host.disable", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                emitLogLineForAction(kv, "disable host");
                String hosts = kv.getValue("hosts");
                try {
                    spawn.toggleHosts(hosts, true);
                    link.sendJSON(200, "OK", json("success",true));
                } catch (Exception e) {
                    link.sendJSON(500, "Error", json("error",e.getMessage()));
                }
            }
        });
        server.mapService("/job.taskdelete", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                emitLogLineForAction(kv, "move job");
                try {
                    String job = kv.getValue("job");
                    int node = Integer.parseInt(kv.getValue("node"));
                    String sourceHostUUID = kv.getValue("source");
                    boolean isReplica = kv.hasKey("rep") ? kv.getValue("rep").equals("1") : true;
                    spawn.deleteTask(job, sourceHostUUID, node, isReplica);
                    link.sendJSON(200, "OK", json("success",true));
                } catch (Exception e) {
                    link.sendJSON(500, "Error", json("error",e.getMessage()));
                }
            }
        });
        server.mapService("/jobs.toautobalance", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                link.sendShortReply(200, "ok", spawn.getJobsToAutobalance().toString());
            }
        });
        server.mapService("/hostfailworker.setObeyTaskSlots", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                KVPairs kv = link.getRequestValues();
                boolean obey = kv.getIntValue("obey", 1) == 1;
                spawn.getHostFailWorker().setObeyTaskSlots(obey);
                link.sendShortReply(200, "ok", "set");
            }
        });
        server.mapService("/task.truesize", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                try {
                    KVPairs kv = link.getRequestValues();
                    String job = kv.getValue("id");
                    int node = Integer.parseInt(kv.getValue("node"));
                    link.sendJSON(200, "OK", json("taskSize",Long.toString(spawn.getTaskTrueSize(job, node))));
                } catch (Exception e) {
                    link.sendJSON(500, "Error", json("error",e.getMessage()));
                }
            }
        });
        server.mapService("/datastore.cutover", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                try {
                    if (!systemManager.isQuiesced()) {
                        link.sendShortReply(500, "Server Error", new JSONObject().put("error", "Spawn is not quiesced").toString());
                    }
                    KVPairs kv = link.getRequestValues();
                    DataStoreUtil.DataStoreType srcType = DataStoreUtil.DataStoreType.valueOf(kv.getValue("src"));
                    DataStoreUtil.DataStoreType tarType = DataStoreUtil.DataStoreType.valueOf(kv.getValue("tar"));
                    boolean checkAllWrites = kv.getIntValue("checkAll", 1) == 1;
                    if (srcType != null || tarType != null) {
                        DataStoreUtil.cutoverBetweenDataStore(DataStoreUtil.makeSpawnDataStore(srcType), DataStoreUtil.makeSpawnDataStore(tarType), checkAllWrites);
                        link.sendJSON(200, "OK", json("success", "transfer complete"));
                    } else {
                        link.sendJSON(500, "Error", json("error", "Please specify a source and target datastore"));
                    }
                } catch (Exception e) {
                    link.sendShortReply(500, "Server Error", new JSONObject().put("error", e.getMessage()).toString());
                }
            }
        });
        server.mapService("/jobs.saveall", new HTTPService() {
            @Override
            public void httpService(HTTPLink link) throws Exception {
                try {
                    // Primarily for use in emergencies where updates have not been sent to the data store for a while
                    spawn.saveAllJobs();
                    link.sendJSON(200, "OK", json("success", "job save operation complete"));

                } catch (Exception ex) {
                    link.sendJSON(500, "Server Error", json("error", ex.toString()));
                    log.trace("Save all jobs exception", ex);
                }
            }
        });
    }

    /**
     * simple kvpairs wrapper service
     */
    private abstract static class KVService extends HTTPService {

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
                link.sendJSON(200, "OK", ret);
            } catch (Exception e) {
                link.sendJSON(500, "Error", json("error",e.getMessage()));
                log.trace("500 Error", e);
            }
        }

    }

    private void updateJobFromCall(HTTPLink link, Spawn spawn) throws Exception {
        KVPairs kv = link.getRequestValues();
        kv.addValue("request.host", link.request().getRemoteHost());
        updateJobFromCall(kv, spawn);
    }

    private void updateJobFromCall(KVPairs kv, Spawn spawn) throws Exception {
        log.debug("[job.update] {}", kv);
        String id = kv.getValue("id", kv.getValue("job"));
        HTTPService.require(id != null, "missing job id");
        IJob job = spawn.getJob(id);
        HTTPService.require(job != null, "invalid job id");
        /** basic command validation */
        String commandName = kv.getValue("command", job.getCommand());
        HTTPService.require(commandName != null, "missing command key");
        HTTPService.require(spawn.getJobCommandManager().getEntity(commandName) != null, "invalid command key");
        job.setCommand(commandName);
        /** update other top-level basic meta-data */
        job.setOwner(kv.getValue("owner", job.getOwner()));
        job.setPriority(kv.getIntValue("priority", job.getPriority()));
        job.setDescription(kv.getValue("description", kv.getValue("desc", job.getDescription())));
        job.setOnCompleteURL(kv.getValue("onComplete", job.getOnCompleteURL()));
        job.setOnErrorURL(kv.getValue("onError", job.getOnErrorURL()));
        job.setOnCompleteTimeout(kv.getIntValue("onCompleteTimeout", job.getOnCompleteTimeout()));
        job.setOnErrorTimeout(kv.getIntValue("onErrorTimeout", job.getOnErrorTimeout()));
        
        spawn.setJobConfig(id, kv.getValue("config", spawn.getJobConfig("id")));
        job.setMaxRunTime(HTTPService.getValidLong(kv, "maxrun", job.getMaxRunTime()));
        job.setRekickTimeout(HTTPService.getValidLong(kv, "rekick", job.getRekickTimeout()));
        job.setEnabled(kv.getIntValue("enable", job.isEnabled() ? 1 : 0) == 1);
        job.setDailyBackups(kv.getIntValue("dailyBackups", job.getDailyBackups()));
        job.setHourlyBackups(kv.getIntValue("hourlyBackups", job.getHourlyBackups()));
        job.setWeeklyBackups(kv.getIntValue("weeklyBackups", job.getWeeklyBackups()));
        job.setMonthlyBackups(kv.getIntValue("monthlyBackups", job.getMonthlyBackups()));
        job.setReplicas(kv.getIntValue("replicas", job.getReplicas()));
        job.setDontDeleteMe(kv.getIntValue("dontDeleteMe", job.getDontDeleteMe() ? 1 : 0) > 0);
        job.setDontCloneMe(kv.getIntValue("dontCloneMe", job.getDontCloneMe() ? 1 : 0) > 0);
        job.setDontAutoBalanceMe(kv.getIntValue("dontAutoBalanceMe", job.getDontAutoBalanceMe() ? 1 : 0) > 0);
        job.setMaxSimulRunning(kv.getIntValue("maxSimulRunning", job.getMaxSimulRunning()));
        job.setMinionType(kv.getValue("minionType", job.getMinionType()));
        job.setAutoRetry(KVUtils.getBooleanValue(kv, job.getAutoRetry(), "autoRetry"));

        // queryConfig paramters
        JobQueryConfig jqc = null;
        if (job.getQueryConfig() != null) {
            jqc = new JobQueryConfig(job.getQueryConfig());
        } else {
            jqc = new JobQueryConfig();
        }

        if (kv.hasKey("qc_canQuery")) {
            jqc.setCanQuery(kv.getValue("qc_canQuery", "true").equals("true"));
        }
        job.setQueryConfig(jqc);

        /**
         * collect / merge parameters
         */
        Map<String, String> setParams = new LinkedHashMap<>();
        /** copy values existing in job parameters */
        if (job.getParameters() != null) {
            // remove specified parameters
            for (Iterator<JobParameter> jp = job.getParameters().iterator(); jp.hasNext();) {
                JobParameter param = jp.next();
                if (kv.hasKey("rp_" + param.getName())) {
                    jp.remove();
                }
            }
            // pull in previous values
            for (JobParameter param : job.getParameters()) {
                setParams.put(param.getName(), param.getValue());
            }
        }
        /** set specified parameters */
        for (KVPair kvp : kv) {
            if (kvp.getKey().startsWith("sp_")) {
                setParams.put(kvp.getKey().substring(3), kvp.getValue());
            }
        }
        /** set params from hash and build new param set */
        setJobParameters(spawn, job, setParams);
        /** update job */
        spawn.updateJob(job);
    }

    public static void setJobParameters(Spawn spawn, IJob job, Map<String, String> setParams)
            throws TokenReplacerOverflowException {
        /** set params from hash and build new param set */
        String expandedConfig = JobExpand.macroExpand(spawn, spawn.getJobConfig(job.getId()));
        Map<String, JobParameter> macroParams = JobExpand.macroFindParameters(expandedConfig);
        ArrayList<JobParameter> newparams = new ArrayList<>(macroParams.size());
        for (JobParameter param : macroParams.values()) {
            param.setValue(setParams.get(param.getName()));
            newparams.add(param);
        }
        job.setParameters(newparams);
    }

    private static void emitLogLineForAction(KVPairs kv, String desc) {
        String user = kv.getValue("user", defaultUser);
        log.warn("User " + user + " initiated action: " + desc);
    }
}
