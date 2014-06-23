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
package com.addthis.hydra.job.spawn.resources;

import javax.annotation.Nonnull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.addthis.basis.kv.KVPair;
import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.Strings;
import com.addthis.basis.util.TokenReplacerOverflowException;

import com.addthis.codec.CodecExceptionLineNumber;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobExpand;
import com.addthis.hydra.job.JobParameter;
import com.addthis.hydra.job.JobQueryConfig;
import com.addthis.hydra.job.JobState;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskReplica;
import com.addthis.hydra.job.Minion;
import com.addthis.hydra.job.RebalanceOutcome;
import com.addthis.hydra.job.Spawn;
import com.addthis.hydra.job.SpawnHttp;
import com.addthis.hydra.job.backup.ScheduledBackupType;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.JobAlert;
import com.addthis.hydra.job.spawn.JobAlertRunner;
import com.addthis.hydra.job.spawn.jersey.User;
import com.addthis.hydra.task.run.JsonRunner;
import com.addthis.hydra.task.run.TaskRunnable;
import com.addthis.hydra.util.DirectedGraph;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Optional;

import com.yammer.dropwizard.auth.Auth;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
@Path("/job")
public class JobsResource {

    private static Logger log = LoggerFactory.getLogger(JobsResource.class);

    private final Spawn spawn;
    private JobAlertRunner jobAlertRunner;
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String defaultUser = "UNKNOWN_USER";

    private static final Pattern comments = Pattern.compile("(?m)^\\s*//\\s*host(?:s)?\\s*:\\s*(.*?)$");

    public JobsResource(Spawn spawn) {
        this.spawn = spawn;
    }

    private static Response buildServerError(Exception exception) {
        log.warn("", exception);
        String message = exception.getMessage();
        if (message == null) {
            message = exception.toString();
        }
        return Response.serverError().entity(message).build();
    }

    @GET
    @Path("/enable")
    @Produces(MediaType.APPLICATION_JSON)
    public Response enableJob(@QueryParam("jobs") String jobarg,
            @QueryParam("enable") @DefaultValue("1") String enableParam,
            @Auth User user) {
        boolean enable = enableParam.equals("1");
        if (jobarg != null) {
            String joblist[] = Strings.splitArray(jobarg, ",");
            emitLogLineForAction(user.getUsername(), (enable ? "enable" : "disable") + " jobs " + jobarg);
            for (String jobid : joblist) {
                IJob job = spawn.getJob(jobid);
                if (job != null && job.setEnabled(enable)) {
                    try {
                        spawn.updateJob(job);
                    } catch (Exception ex) {
                        return buildServerError(ex);
                    }
                }
            }
            return Response.ok().build();
        } else {
            return Response.serverError().entity("Missing job ids").build();
        }
    }

    @GET
    @Path("/rebalance")
    @Produces(MediaType.TEXT_PLAIN)
    public Response rebalanceJob(@QueryParam("id") String id, @Auth User user, @QueryParam("tasksToMove") @DefaultValue("-1") Integer tasksToMove) {
        emitLogLineForAction(user.getUsername(), "job rebalance on " + id + " tasksToMove=" + tasksToMove);
        try {
            RebalanceOutcome ro = spawn.rebalanceJob(id, tasksToMove);
            String outcome = ro.toString();
            return Response.ok(outcome).build();
        } catch (Exception ex)  {
            log.warn("", ex);
            return Response.serverError().entity("Rebalance Error: " + ex.getMessage()).build();
        }
    }

    @POST
    @Path("/expand")
    @Produces(MediaType.APPLICATION_JSON)
    public Response expandJobPost(@QueryParam("pairs") KVPairs kv) throws Exception {
        try {
            String expandedConfig = configExpansion(kv);

            return Response.ok("attachment; filename=expanded_job.json",
                    MediaType.APPLICATION_OCTET_STREAM)
                    .entity(expandedConfig)
                    .header("topic", "expanded_job")
                    .build();

        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    private String configExpansion(KVPairs kv) throws TokenReplacerOverflowException {
        String id, jobConfig, expandedConfig;

        KVPair idPair = kv.removePair("id");
        id = (idPair == null) ? null : idPair.getValue();
        jobConfig = kv.removePair("config").getValue();

        expandedConfig = JobExpand.macroExpand(spawn, jobConfig);
        Map<String, JobParameter> parameters = JobExpand.macroFindParameters(expandedConfig);

        for (KVPair pair : kv) {
            String key = pair.getKey().substring(3);
            String value = pair.getValue();
            JobParameter param = parameters.get(key);
            if (param != null) {
                param.setValue(value);
            } else {
                param = new JobParameter();
                param.setName(key);
                param.setValue(value);
                parameters.put(key, param);
            }
        }

        expandedConfig = spawn.expandJob(id, parameters.values(), jobConfig);
        return expandedConfig;
    }


    /**
     * expand the job's macros and send the text to the user
     */
    @GET
    @Path("/expand")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response expandJobGet(@QueryParam("id") @DefaultValue("") String id) {
        if ("".equals(id)) {
            return Response.status(Response.Status.NOT_FOUND)
                    .header("topic", "Expansion Error")
                    .entity("{error:'unable to expand job, job id must be non null and not empty'}")
                    .build();
        } else {
            try {
                String expandedJobConfig = spawn.expandJob(id);
                return Response.ok("attachment; filename=expanded_job.json", MediaType.APPLICATION_OCTET_STREAM)
                        .entity(expandedJobConfig)
                        .header("topic", "expanded_job")
                        .build();
            } catch (Exception ex) {
                return buildServerError(ex);
            }
        }
    }


    @GET
    @Path("/synchronize")
    @Produces(MediaType.APPLICATION_JSON)
    /** url called via ajax by client to rebalance a job */
    public Response synchronizeJob(@QueryParam("id") @DefaultValue("") String id,
            @QueryParam("user") Optional<String> user) {
        emitLogLineForAction(user.or(defaultUser), "job synchronize on " + id);
        if (spawn.synchronizeJobState(id)) {
            return Response.ok("{id:'" + id + "',action:'synchronzied'}").build();
        } else {
            log.warn("[job.synchronize] " + id + " unable to synchronize job");
            return Response.status(Response.Status.NOT_FOUND)
                    .header("topic", "Synchronize Error")
                    .entity("{error:'unable to synchronize job, check spawn log file for more details'}")
                    .build();
        }
    }

    @GET
    @Path("/delete") //TODO: should this be a @delete?
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteJob(@QueryParam("id") @DefaultValue("") String id,
            @QueryParam("user") Optional<String> user) {
        Job job = spawn.getJob(id);

        if (job != null && !job.getState().equals(JobState.IDLE)) {
            return Response.serverError().entity("A non IDLE job cannot be deleted").build();
        } else {
            emitLogLineForAction(user.or(defaultUser), "job delete on " + id);
            try {
                Spawn.DeleteStatus status = spawn.deleteJob(id);
                switch (status) {
                    case SUCCESS:
                        return Response.ok().build();
                    case JOB_MISSING:
                        log.warn("[job.delete] " + id + " missing job");
                        return Response.status(Response.Status.NOT_FOUND).build();
                    case JOB_DO_NOT_DELETE:
                        return Response.status(Response.Status.NOT_MODIFIED).build();
                    default:
                        throw new IllegalStateException("Delete status " + status + " is not recognized");
                }
            } catch (Exception ex) {
                return buildServerError(ex);
            }
        }
    }

    @GET
    @Path("/test")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response test(@QueryParam("pairs") KVPairs params) {
        return Response.ok("{\"foo\":\"bar\"}").build();
    }

    @GET
    @Path("/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listJobs() {
        JSONArray jobs = new JSONArray();
        try {
            for (IJob job : spawn.listJobsConcurrentImmutable()) {
                JSONObject jobUpdateEvent = spawn.getJobUpdateEvent(job);
                jobs.put(jobUpdateEvent);
            }
            return Response.ok(jobs.toString()).build();
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    private JSONObject dependencyGraphNode(@Nonnull IJob job) throws Exception {
        JSONObject newNode = job.toJSON();
        newNode.remove("config");
        newNode.remove("nodes");
        return newNode;
    }

    private JSONObject dependencyGraphEdge(@Nonnull String source, @Nonnull String sink)
            throws JSONException {
        JSONObject newEdge = new JSONObject();
        newEdge.put("source", source);
        newEdge.put("sink", sink);
        return newEdge;
    }

    @GET
    @Path("/dependencies/sources")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSourceDependencies(@QueryParam("id") @DefaultValue("") String id) {
        try {
            JSONObject returnGraph = new JSONObject();
            JSONArray nodes = new JSONArray();
            JSONArray edges = new JSONArray();

            DirectedGraph<String> dependencies = spawn.getJobDependencies();
            Set<String> jobIds = dependencies.sourcesClosure(id);

            for (String jobId : jobIds) {
                IJob job = spawn.getJob(jobId);
                if (job != null) {
                    nodes.put(dependencyGraphNode(job));
                    Set<String> sourceEdges = dependencies.getSourceEdges(jobId);
                    if (sourceEdges != null) {
                        for (String edge : sourceEdges) {
                            edges.put(dependencyGraphEdge(edge, jobId));
                        }
                    }
                }
            }

            returnGraph.put("nodes", nodes);
            returnGraph.put("edges", edges);
            return Response.ok(returnGraph.toString()).build();
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    @GET
    @Path("/dependencies/sinks")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSinkDependencies(@QueryParam("id") @DefaultValue("") String id) {
        try {
            JSONObject returnGraph = new JSONObject();
            JSONArray nodes = new JSONArray();
            JSONArray edges = new JSONArray();

            DirectedGraph<String> dependencies = spawn.getJobDependencies();
            Set<String> jobIds = dependencies.sinksClosure(id);

            for (String jobId : jobIds) {
                IJob job = spawn.getJob(jobId);
                if (job != null) {
                    nodes.put(dependencyGraphNode(job));
                    Set<String> sinkEdges = dependencies.getSinkEdges(jobId);
                    if (sinkEdges != null) {
                        for (String edge : sinkEdges) {
                            edges.put(dependencyGraphEdge(jobId, edge));
                        }
                    }
                }
            }

            returnGraph.put("nodes", nodes);
            returnGraph.put("edges", edges);
            return Response.ok(returnGraph.toString()).build();
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    @GET
    @Path("/dependencies/connected")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConnectedDependencies(@QueryParam("id") @DefaultValue("") String id) {
        try {
            JSONObject returnGraph = new JSONObject();
            JSONArray nodes = new JSONArray();
            JSONArray edges = new JSONArray();

            DirectedGraph<String> dependencies = spawn.getJobDependencies();
            Set<String> jobIds = dependencies.transitiveClosure(id);

            for (String jobId : jobIds) {
                IJob job = spawn.getJob(jobId);
                if (job != null) {
                    nodes.put(dependencyGraphNode(job));
                }
            }
            Set<DirectedGraph.Edge<String>> edgeSet = dependencies.getAllEdges(jobIds);
            for (DirectedGraph.Edge<String> edge : edgeSet) {
                edges.put(dependencyGraphEdge(edge.source, edge.sink));
            }
            returnGraph.put("nodes", nodes);
            returnGraph.put("edges", edges);
            return Response.ok(returnGraph.toString()).build();
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    @GET
    @Path("/dependencies/all")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConnectedDependencies() {
        try {
            JSONObject returnGraph = new JSONObject();
            JSONArray nodes = new JSONArray();
            JSONArray edges = new JSONArray();

            DirectedGraph<String> dependencies = spawn.getJobDependencies();
            Set<String> jobIds = dependencies.getNodes();

            for (String jobId : jobIds) {
                IJob job = spawn.getJob(jobId);
                if (job != null) {
                    nodes.put(dependencyGraphNode(job));
                }
            }
            Set<DirectedGraph.Edge<String>> edgeSet = dependencies.getAllEdges(jobIds);
            for (DirectedGraph.Edge<String> edge : edgeSet) {
                edges.put(dependencyGraphEdge(edge.source, edge.sink));
            }
            returnGraph.put("nodes", nodes);
            returnGraph.put("edges", edges);
            return Response.ok(returnGraph.toString()).build();
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    @GET
    @Path("/alerts.toggle")
    @Produces(MediaType.APPLICATION_JSON)
    public Response toggleJobAlerts(@QueryParam("enable") @DefaultValue("true") Boolean enable) {
        try {
            if (enable) {
                spawn.enableAlerts();
            } else {
                spawn.disableAlerts();
            }
        } catch (Exception e) {
            log.warn("Failed to toggle alerts due to : " + e.getMessage());
            return Response.ok("false").build();
        }

        return Response.ok("true").build();
    }

    @GET
    @Path("/get")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJob(@QueryParam("id") String id, @QueryParam("field") Optional<String> field) {
        try {
            IJob job = spawn.getJob(id);
            if (job != null) {
                JSONObject jobobj = job.toJSON();
                if (field.isPresent()) {
                    Object fieldObject = "config".equals(field.get()) ? spawn.getJobConfig(id) : jobobj.get(field.get());
                    if (fieldObject != null) {
                        return Response.ok(fieldObject.toString()).build();
                    }
                }
                return Response.ok(jobobj.toString()).build();
            } else {
                return Response.status(Response.Status.NOT_FOUND)
                        .header("topic", "No Job")
                        .entity("no such job found with id " + id)
                        .build();
            }
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    @POST
    @Path("/save")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_FORM_URLENCODED, MediaType.WILDCARD})
    @Produces({MediaType.APPLICATION_JSON})
    public Response saveJob(@QueryParam("pairs") KVPairs kv, @Auth User user) throws Exception {
        IJob job;
        if (!kv.hasKey("id") || Strings.isEmpty(kv.getValue("id"))) {
            job = spawn.createJob(
                    kv.getValue("owner", user.getUsername()),
                    kv.getIntValue("nodes", -1),
                    Arrays.asList(Strings.splitArray(kv.getValue("hosts", ""), ",")),
                    kv.getValue("minionType", Minion.getDefaultMinionType()),
                    kv.getValue("command"));
            kv.addValue("id", job.getId());
        } else {
            job = spawn.getJob(kv.getValue("id"));
        }
//      kv.addValue("request.host", request.get);
        job.setCommand(kv.getValue("command", job.getCommand()));
        /** update other top-level basic meta-data */

        job.setOwner(kv.getValue("owner", job.getOwner()));

        job.setPriority(kv.getIntValue("priority", job.getPriority()));

        job.setDescription(kv.getValue("description", job.getDescription()));
        job.setOnCompleteURL(kv.getValue("onComplete", job.getOnCompleteURL()));
        job.setOnErrorURL(kv.getValue("onError", job.getOnErrorURL()));
        job.setOnCompleteTimeout(kv.getIntValue("onCompleteTimeout", job.getOnCompleteTimeout()));
        job.setOnErrorTimeout(kv.getIntValue("onErrorTimeout", job.getOnErrorTimeout()));

        spawn.setJobConfig(job.getId(), kv.getValue("config", spawn.getJobConfig(job.getId())));

        job.setMaxRunTime(SpawnHttp.HTTPService.getValidLong(kv, "maxrun", job.getMaxRunTime()));
        job.setRekickTimeout(SpawnHttp.HTTPService.getValidLong(kv, "rekick", job.getRekickTimeout()));

        job.setEnabled(kv.getIntValue("enable", job.isEnabled() ? 1 : 0) == 1);
        job.setKillSignal(kv.getValue("logkill", job.getKillSignal()));
        job.setBackups(kv.getIntValue("backups", job.getBackups()));
        job.setDailyBackups(kv.getIntValue("dailyBackups", job.getDailyBackups()));
        job.setHourlyBackups(kv.getIntValue("hourlyBackups", job.getHourlyBackups()));
        job.setWeeklyBackups(kv.getIntValue("weeklyBackups", job.getWeeklyBackups()));
        job.setMonthlyBackups(kv.getIntValue("monthlyBackups", job.getMonthlyBackups()));
        job.setReplicas(kv.getIntValue("replicas", job.getReplicas()));

        job.setReadOnlyReplicas(kv.getIntValue("readOnlyReplicas", job.getReadOnlyReplicas()));
        job.setReplicationFactor(kv.getIntValue("replicationFactor", job.getReplicationFactor()));
        job.setStomp(kv.getIntValue("stomp", job.getStomp() ? 1 : 0) == 1);
        job.setDontDeleteMe(kv.getIntValue("dontDeleteMe", job.getDontDeleteMe() ? 1 : 0) > 0);
        job.setDontAutoBalanceMe(kv.getIntValue("dontAutoBalanceMe", job.getDontAutoBalanceMe() ? 1 : 0) > 0);
        job.setMaxSimulRunning(kv.getIntValue("maxSimulRunning", job.getMaxSimulRunning()));
        job.setMinionType(kv.getValue("minionType", job.getMinionType()));
        job.setRetries(kv.getIntValue("retries", job.getRetries()));

        // queryConfig paramters
        JobQueryConfig jqc;
        if (job.getQueryConfig() != null) {
            jqc = job.getQueryConfig().clone();
        } else {
            jqc = new JobQueryConfig();
        }

        if (kv.hasKey("qc_canQuery")) {
            jqc.setCanQuery(kv.getValue("qc_canQuery", "true").equals("true"));
        }
        if (kv.hasKey("qc_queryTraceLevel")) {
            jqc.setQueryTraceLevel(kv.getIntValue("qc_queryTraceLevel", 0));
        }
        if (kv.hasKey("qc_consecutiveFailureThreshold")) {
            jqc.setConsecutiveFailureThreshold(kv.getIntValue("qc_consecutiveFailureThreshold", 100));
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
        try {
            /** set params from hash and build new param set */
            setJobParameters(spawn, job, setParams);
            /** update job */
            spawn.updateJob(job);
            spawn.submitConfigUpdate(kv.getValue("id"), kv.getValue("commit"));
        } catch (Exception ex) {
            return buildServerError(ex);
        }
        return Response.ok("{\"id\":\"" + kv.getValue("id") + "\",\"updated\":\"true\"}").build();
    }

    public static void setJobParameters(Spawn spawn, IJob job, Map<String, String> setParams)
            throws TokenReplacerOverflowException {
        /** set params from hash and build new param set */
        String expandedConfig = JobExpand.macroExpand(spawn, spawn.getJobConfig(job.getId()));
        Map<String, JobParameter> macroParams = JobExpand.macroFindParameters(expandedConfig);
        ArrayList<JobParameter> newparams = new ArrayList<>(macroParams.size());
        for (JobParameter param : macroParams.values()) {
            String name = param.getName();
            String value = setParams.get(name);
            param.setValue(value);
            newparams.add(param);
        }
        job.setParameters(newparams);
    }

    @GET
    @Path("/revert")
    @Produces(MediaType.APPLICATION_JSON)
    public Response revertJob(@QueryParam("id") String id,
            @QueryParam("type") @DefaultValue("gold") String type,
            @QueryParam("revision") @DefaultValue("-1") Integer revision,
            @QueryParam("node") @DefaultValue("-1") Integer node,
            @QueryParam("time") @DefaultValue("-1") Long time,
            @Auth User user) {
        try {
            emitLogLineForAction(user.getUsername(), "job revert on " + id + " of type " + type);
            IJob job = spawn.getJob(id);
            spawn.revertJobOrTask(job.getId(), node, type, revision, time);
            return Response.ok("{\"id\":\"" + job.getId() + "\", \"action\":\"reverted\"}").build();
        } catch (Exception ex) {
            ex.printStackTrace();
            return Response.serverError().entity(ex).build();
        }
    }

    @GET
    @Path("/backups.list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRevisions(@QueryParam("id") String id, @QueryParam("node") @DefaultValue("-1") int node, @Auth User user) {
        try {
            if (spawn.isSpawnMeshAvailable()) {
                IJob job = spawn.getJob(id);
                Map<ScheduledBackupType, SortedSet<Long>> backupDates = spawn.getJobBackups(job.getId(), node);
                String jsonString = mapper.writeValueAsString(backupDates);
                return Response.ok(jsonString).build();
            } else {
                return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Spawn Mesh is not available.").build();
            }
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }


    @GET
    @Path("/tasks.get")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobTasks(@QueryParam("id") String id) {
        try {
            IJob job = spawn.getJob(id);
            if (job != null) {
                JSONArray tasksJson = new JSONArray();
                for (JobTask task : job.getCopyOfTasks()) {
                    HostState host = spawn.getHostState(task.getHostUUID());
                    JSONObject json = task.toJSON();
                    json.put("host", host.getHost());
                    json.put("hostPort", host.getPort());

                    JSONArray taskReplicas = new JSONArray();
                    for (JobTaskReplica replica : task.getAllReplicas()) {
                        JSONObject replicaJson = new JSONObject();
                        HostState replicaHost = spawn.getHostState(replica.getHostUUID());
                        replicaJson.put("hostUrl", replicaHost.getHost());
                        replicaJson.put("hostPort", replicaHost.getPort());
                        replicaJson.put("lastUpdate", replica.getLastUpdate());
                        replicaJson.put("version", replica.getVersion());
                        taskReplicas.put(replicaJson);
                    }
                    json.put("replicas", taskReplicas);

                    tasksJson.put(json);
                }
                return Response.ok(tasksJson.toString()).build();
            } else {
                return Response.status(Response.Status.NOT_FOUND).header("topic", "No Job").build();
            }
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    @GET
    @Path("/secret")
    public Response getSecret(@Auth User user) {
        return Response.ok(user.getUsername()).build();
    }

    private void submitJobHelper(String jobId, int taskId) throws Exception {
        if (taskId < 0) {
            spawn.startJob(jobId, true);
        } else {
            spawn.startTask(jobId, taskId, true, true, false);
        }
    }

    /**
     * Support for both the undocumented API that uses "jobid" and "select"
     * and the documented API that uses "id" and "task".
     */
    @GET
    @Path("/start")
    @Produces(MediaType.APPLICATION_JSON)
    public Response submitJob(@QueryParam("jobid") Optional<String> jobIds,
            @QueryParam("select") @DefaultValue("-1") int select,
            @QueryParam("id") Optional<String> id,
            @QueryParam("task") @DefaultValue("-1") int task) {
        try {
            if (jobIds.isPresent()) {
                String joblist[] = Strings.splitArray(jobIds.get(), ",");
                for (String aJob : joblist) {
                    submitJobHelper(aJob, select);
                }
                return Response.ok("{\"id\":\"" + jobIds.get() + "\",  \"updated\": \"true\"}").build();
            } else if (id.isPresent()) {
                submitJobHelper(id.get(), task);
                return Response.ok("{\"id\":\"" + id.get() + "\",  \"updated\": \"true\"}").build();
            } else {
                return Response.status(Response.Status.BAD_REQUEST).entity("job id not specified").build();
            }

        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    @GET
    @Path("/checkJobDirs")
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkJobDirs(@QueryParam("id") String id, @QueryParam("node") @DefaultValue("-1") Integer node) {
        try {
            IJob job = spawn.getJob(id);
            if (job != null) {

                return Response.ok(spawn.checkTaskDirJSON(id, node).toString()).build();
            } else {
                return Response.status(Response.Status.NOT_FOUND).header("topic", "No Job").build();
            }

        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    @GET
    @Path("/fixJobDirs")
    @Produces(MediaType.APPLICATION_JSON)
    public Response fixJobDirs(@QueryParam("id") String id, @QueryParam("node") @DefaultValue("-1") int node) {
        try {
            IJob job = spawn.getJob(id);
            if (job != null) {
                return Response.ok(spawn.fixTaskDir(id, node, false, false)).build();
            } else {
                return Response.status(Response.Status.NOT_FOUND).header("topic", "No Job").build();
            }

        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    @GET
    @Path("/history")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJobHistory(@QueryParam("id") String id) {
        return Response.ok(spawn.getJobHistory(id).toString()).build();
    }

    @GET
    @Path("/config.view")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getJobHistoricalConfig(@QueryParam("id") String id, @QueryParam("commit") String commit) {
        return Response.ok(spawn.getJobHistoricalConfig(id, commit)).build();
    }

    @GET
    @Path("/config.diff")
    @Produces(MediaType.TEXT_PLAIN)
    public Response diffConfig(@QueryParam("id") String id, @QueryParam("commit") String commit) {
        return Response.ok(spawn.diff(id, commit)).build();
    }

    private String[] generateHosts(String hosts, String config) {
        if (hosts.equals("") && (config != null)) {
            Matcher matcher = comments.matcher(config);
            while (matcher.find()) {
                String ids = matcher.group(1);
                try {
                    if (ids.length() > 1) {
                        if (ids.charAt(0) == '"' && ids.charAt(ids.length() - 1) == '"') {
                            String[] retval = new String[1];
                            retval[0] = ids.substring(1, ids.length() - 1);
                            return retval;
                        } else if (ids.charAt(0) == '[') {
                            JSONArray json = new JSONArray(ids);
                            String[] retval = new String[json.length()];
                            for (int i = 0; i < retval.length; i++) {
                                retval[i] = json.getString(i);
                            }
                            return retval;
                        }
                    }
                } catch (JSONException ex) {
                    log.warn("Failed to parse input " + ids);
                }
            }
        }
        return Strings.splitArray(hosts, ",");
    }

    @POST
    @Path("/submit")
    @Produces(MediaType.APPLICATION_JSON)
    public Response submitJob(@QueryParam("pairs") KVPairs kv, @Auth User user) {
        try {
            if (log.isDebugEnabled()) log.debug("job.submit --> " + kv.toString());
            if (kv.count() > 0) {
                boolean schedule = kv.getValue("spawn", "0").equals("1");
                boolean manual = kv.getValue("manual", "0").equals("1");
                String id = kv.getValue("id", "");
//              emitLogLineForAction(kv, "submit job " + id);
                if (Strings.isEmpty(id) && !schedule) {
                    String[] hosts = generateHosts(kv.getValue("hosts", ""), kv.getValue("config"));
                    IJob job = spawn.createJob(
                            kv.getValue("owner", user.getUsername()),
                            kv.getIntValue("nodes", -1),
                            Arrays.asList(hosts),
                            kv.getValue("minionType", Minion.getDefaultMinionType()),
                            kv.getValue("command"));
                    kv.addValue("id", job.getId());
                }
                updateJobFromCall(kv);
                if (id != null && schedule) {
                    int select = kv.getIntValue("select", -1);
                    if (select >= 0) {
                        spawn.startTask(id, select, true, manual, false);
                    } else {
                        spawn.startJob(id, manual);
                    }
                }
                return Response.ok("{\"id\":\"" + kv.getValue("id") + "\",  \"submitted\": \"true\"}").build();
            }
            return Response.status(Response.Status.BAD_REQUEST).entity("no job fields received").build();
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    private void updateJobFromCall(KVPairs kv) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("[job.update] " + kv);
        }
        String id = kv.getValue("id", kv.getValue("job"));
        SpawnHttp.HTTPService.require(id != null, "missing job id");
        IJob job = spawn.getJob(id);
        SpawnHttp.HTTPService.require(job != null, "invalid job id");
        /** basic command validation */
        String commandName = kv.getValue("command", job.getCommand());
        SpawnHttp.HTTPService.require(commandName != null, "missing command key");
        SpawnHttp.HTTPService.require(spawn.getCommand(commandName) != null, "invalid command key");
        job.setCommand(commandName);
        /** update other top-level basic meta-data */
        job.setOwner(kv.getValue("owner", job.getOwner()));
        job.setPriority(kv.getIntValue("priority", job.getPriority()));
        job.setDescription(kv.getValue("description", job.getDescription()));
        job.setOnCompleteURL(kv.getValue("onComplete", job.getOnCompleteURL()));
        job.setOnErrorURL(kv.getValue("onError", job.getOnErrorURL()));
        job.setOnCompleteTimeout(kv.getIntValue("onCompleteTimeout", job.getOnCompleteTimeout()));
        job.setOnErrorTimeout(kv.getIntValue("onErrorTimeout", job.getOnErrorTimeout()));

        spawn.setJobConfig(id, kv.getValue("config"));
        job.setEnabled(kv.getIntValue("enable", job.isEnabled() ? 1 : 0) == 1);
        job.setKillSignal(kv.getValue("logkill", job.getKillSignal()));
        job.setBackups(kv.getIntValue("backups", job.getBackups()));
        job.setDailyBackups(kv.getIntValue("dailyBackups", job.getDailyBackups()));
        job.setHourlyBackups(kv.getIntValue("hourlyBackups", job.getHourlyBackups()));
        job.setWeeklyBackups(kv.getIntValue("weeklyBackups", job.getWeeklyBackups()));
        job.setMonthlyBackups(kv.getIntValue("monthlyBackups", job.getMonthlyBackups()));
        job.setReplicas(kv.getIntValue("replicas", job.getReplicas()));
        job.setReadOnlyReplicas(kv.getIntValue("readOnlyReplicas", job.getReadOnlyReplicas()));
        job.setReplicationFactor(kv.getIntValue("replicationFactor", job.getReplicationFactor()));
        job.setMaxSimulRunning(kv.getIntValue("maxSimulRunning", job.getMaxSimulRunning()));
        job.setMinionType(kv.getValue("minionType", job.getMinionType()));
        job.setStomp(kv.getIntValue("stomp", job.getStomp() ? 1 : 0) == 1);
        job.setRetries(kv.getIntValue("retries", job.getRetries()));
        //for deprecation purposes
        boolean dontDelete = kv.getValue("dontDeleteMe", "false").equals("true") || kv.getValue("dontDeleteMe", "0").equals("1");
        job.setDontDeleteMe(dontDelete);
        boolean dontAutoBalance = kv.getValue("dontAutoBalanceMe", "false").equals("true") || kv.getValue("dontAutoBalanceMe", "0").equals("1");
        job.setDontAutoBalanceMe(dontAutoBalance);
        if (kv.hasKey("maxRunTime")) {
            job.setMaxRunTime(SpawnHttp.HTTPService.getValidLong(kv, "maxRunTime", job.getMaxRunTime()));
        } else {
            job.setMaxRunTime(SpawnHttp.HTTPService.getValidLong(kv, "maxrun", job.getMaxRunTime()));
        }
        if (kv.hasKey("rekickTimeout")) {
            job.setRekickTimeout(SpawnHttp.HTTPService.getValidLong(kv, "rekickTimeout", job.getRekickTimeout()));
        } else {
            job.setRekickTimeout(SpawnHttp.HTTPService.getValidLong(kv, "rekick", job.getRekickTimeout()));
        }
        if (kv.hasKey("alerts")) {
            String alertJson = kv.getValue("alerts");
            List<JobAlert> alerts = mapper.readValue(alertJson, TypeFactory.defaultInstance().constructParametricType(List.class, JobAlert.class));
            job.setAlerts(alerts);
        }

        // queryConfig paramters
        JobQueryConfig jqc;
        if (job.getQueryConfig() != null) {
            jqc = job.getQueryConfig().clone();
        } else {
            jqc = new JobQueryConfig();
        }

        if (kv.hasKey("qc_canQuery")) {
            jqc.setCanQuery(kv.getValue("qc_canQuery", "true").equals("true"));
        }
        if (kv.hasKey("qc_queryTraceLevel")) {
            jqc.setQueryTraceLevel(kv.getIntValue("qc_queryTraceLevel", 0));
        }
        if (kv.hasKey("qc_consecutiveFailureThreshold")) {
            jqc.setConsecutiveFailureThreshold(kv.getIntValue("qc_consecutiveFailureThreshold", 100));
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
        spawn.submitConfigUpdate(id, kv.getValue("commit"));
    }

    private Response validateCreateError(String message, JSONArray lines,
            JSONArray columns, String errorType) throws JSONException {
        JSONObject error = new JSONObject();
        error.put("message", message).put("lines", lines).put("columns", columns).put("result", errorType);
        return Response.ok().entity(error.toString()).build();
    }

    private Response validateExpandedConfigurationBody(String expandedConfig)
            throws JSONException {
        JSONArray lineErrors = new JSONArray();
        JSONArray lineColumns = new JSONArray();

        JSONObject jobJSON;

        try {
            jobJSON = new JSONObject(expandedConfig, false);
        } catch (JSONException ex) {
            lineErrors.put(ex.getLine());
            lineColumns.put(ex.getColumn());
            return validateCreateError(ex.getMessage(), lineErrors, lineColumns, "postExpansionError");
        }
        try {
            JsonRunner.initClasses(jobJSON);
            jobJSON.remove("classes");
            List<CodecExceptionLineNumber> warnings = new ArrayList<>();
            CodecJSON.decodeObject(TaskRunnable.class, jobJSON, warnings);
            if (warnings.size() > 0) {
                StringBuilder message = new StringBuilder();
                Iterator<CodecExceptionLineNumber> iter = warnings.iterator();
                while (iter.hasNext()) {
                    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
                    CodecExceptionLineNumber ex = iter.next();
                    lineErrors.put(ex.getLine());
                    lineColumns.put(ex.getColumn());
                    message.append(ex.getMessage());
                    if (iter.hasNext()) {
                        message.append("<br>");
                    }
                }
                return validateCreateError(message.toString(), lineErrors, lineColumns, "postExpansionError");
            }
        } catch (JSONException ex) {
            lineErrors.put(ex.getLine());
            lineColumns.put(ex.getColumn());
            return validateCreateError(ex.getMessage(), lineErrors, lineColumns, "postExpansionError");
        } catch (CodecExceptionLineNumber ex) {
            lineErrors.put(ex.getLine());
            lineColumns.put(ex.getColumn());
            return validateCreateError(ex.getMessage(), lineErrors, lineColumns, "postExpansionError");
        }

        return Response.ok(new JSONObject().put("result", "valid").toString()).build();
    }

    @GET
    @Path("/validate")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response validateJobGet(@QueryParam("id") @DefaultValue("") String id) {
        try {
            if ("".equals(id)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .header("topic", "Expansion Error")
                        .entity("{error:'unable to expand job, job id must be non null and not empty'}")
                        .build();
            } else {
                String expandedConfig;
                try {
                    expandedConfig = spawn.expandJob(id);
                } catch (Exception ex) {
                    JSONArray lineErrors = new JSONArray();
                    JSONArray lineColumns = new JSONArray();
                    String message = ex.getMessage() == null ? ex.toString() : ex.getMessage();
                    return validateCreateError(message, lineErrors, lineColumns, "preExpansionError");
                }

                try {
                    return validateExpandedConfigurationBody(expandedConfig);
                } catch (Exception ex) {
                    JSONArray lineErrors = new JSONArray();
                    JSONArray lineColumns = new JSONArray();

                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    ex.printStackTrace(pw);
                    String msg = sw.toString();
                    int maxLen = Math.min(msg.length(), 600);

                    String message = "Unexpected error encountered. " +
                                     "\n" + msg.substring(0, maxLen) + "...";

                    return validateCreateError(message, lineErrors, lineColumns, "postExpansionError");
                }
            }
        } catch (Exception ex) {
            return Response.serverError().build();
        }
    }

    @POST
    @Path("/validate")
    @Produces(MediaType.APPLICATION_JSON)
    public Response validateJobPost(@QueryParam("pairs") KVPairs kv) throws Exception {
        String expandedConfig;
        try {
            expandedConfig = configExpansion(kv);
        } catch (Exception ex) {
            JSONArray lineErrors = new JSONArray();
            JSONArray lineColumns = new JSONArray();
            String message = ex.getMessage() == null ? ex.toString() : ex.getMessage();
            return validateCreateError(message, lineErrors, lineColumns, "preExpansionError");
        }

        try {
            return validateExpandedConfigurationBody(expandedConfig);
        } catch (Exception ex) {
            JSONArray lineErrors = new JSONArray();
            JSONArray lineColumns = new JSONArray();

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            String msg = sw.toString();
            int maxLen = Math.min(msg.length(), 600);

            String message = "Unexpected error encountered. " +
                             "\n" + msg.substring(0, maxLen) + "...";

            return validateCreateError(message, lineErrors, lineColumns, "postExpansionError");
        }
    }

    boolean stopJobHelper(String id, Optional<Boolean> cancelParam,
            Optional<Boolean> forceParam,
            int nodeId) throws Exception {
        IJob job = spawn.getJob(id);
        if (job == null) {
            return false;
        }
        boolean cancelRekick = cancelParam.or(false);
        boolean force = forceParam.or(false);
        // cancel re-spawning
        if (cancelRekick) {
            job.setRekickTimeout(null);
        }
        log.warn("[job.stop] " + job.getId() + "/" + nodeId + ", cancel=" + cancelRekick + ", force=" + force);
        // broadcast to all hosts if no node specified
        if (nodeId < 0) {
            if (force) {
                spawn.killJob(id);
            } else {
                spawn.stopJob(id);
            }
        } else {
            if (force) {
                spawn.killTask(id, nodeId);
            } else {
                spawn.stopTask(id, nodeId);
            }
        }
        return true;
    }

    /**
     * Support for both the undocumented API that uses "jobid" and "node"
     * and the documented API that uses "id" and "task".
     */
    @GET
    @Path("/stop")
    @Produces(MediaType.APPLICATION_JSON)
    public Response stopJob(@QueryParam("jobid") Optional<String> jobIds,
            @QueryParam("cancel") Optional<Boolean> cancelParam,
            @QueryParam("force") Optional<Boolean> forceParam,
            @QueryParam("node") @DefaultValue("-1") int nodeParam,
            @QueryParam("id") Optional<String> id,
            @QueryParam("task") @DefaultValue("-1") int task) {
        try {
            if (jobIds.isPresent()) {
                String ids = jobIds.get();
                String joblist[] = Strings.splitArray(ids, ",");
                for (String jobName : joblist) {
                    boolean status = stopJobHelper(jobName, cancelParam, forceParam, nodeParam);
                    if (!status) {
                        return Response.status(Response.Status.NOT_FOUND)
                                .header("topic", "Invalid ID")
                                .entity("{error:'no such job'}")
                                .build();
                    }
                }
                return Response.ok("{\"id\":\"" + ids + "\",\"action\":\"stopped\"}").build();
            } else if (id.isPresent()) {
                boolean status = stopJobHelper(id.get(), cancelParam, forceParam, task);
                if (!status) {
                    return Response.status(Response.Status.NOT_FOUND)
                            .header("topic", "Invalid ID")
                            .entity("{error:'no such job'}")
                            .build();
                } else {
                    return Response.ok("{\"id\":\"" + id.get() + "\",\"action\":\"stopped\"}").build();
                }
            } else {
                return Response.status(Response.Status.BAD_REQUEST).entity("job id not specified").build();
            }
        } catch (Exception ex) {
            return buildServerError(ex);
        }

    }

    private static void emitLogLineForAction(String user, String desc) {
        log.warn("User " + user + " initiated action: " + desc);
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
