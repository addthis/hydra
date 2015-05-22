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
package com.addthis.hydra.job.web.resources;

import javax.annotation.Nonnull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.regex.Pattern;

import com.addthis.basis.kv.KVPair;
import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.LessStrings;
import com.addthis.basis.util.TokenReplacerOverflowException;

import com.addthis.codec.config.Configs;
import com.addthis.codec.jackson.CodecJackson;
import com.addthis.codec.jackson.Jackson;
import com.addthis.codec.json.CodecJSON;
import com.addthis.codec.plugins.PluginRegistry;
import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobExpand;
import com.addthis.hydra.job.JobParameter;
import com.addthis.hydra.job.JobState;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskReplica;
import com.addthis.hydra.job.RebalanceOutcome;
import com.addthis.hydra.job.auth.InsufficientPrivilegesException;
import com.addthis.hydra.job.backup.ScheduledBackupType;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.DeleteStatus;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.web.JobRequestHandler;
import com.addthis.hydra.job.web.KVUtils;
import com.addthis.hydra.task.run.TaskRunnable;
import com.addthis.hydra.task.run.TaskRunner;
import com.addthis.hydra.util.DirectedGraph;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigOrigin;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/job")
public class JobsResource {
    private static final Logger log = LoggerFactory.getLogger(JobsResource.class);

    @SuppressWarnings("unused")
    private static final Pattern COMMENTS_REGEX = Pattern.compile("(?m)^\\s*//\\s*host(?:s)?\\s*:\\s*(.*?)$");
    private static final ImmutableSet<String> PERMISSIONS = ImmutableSet.of("no change", "true", "false");
    private static final ImmutableSet<String> MODIFYING_PERMISSIONS = ImmutableSet.of("true", "false");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Spawn spawn;
    private final JobRequestHandler requestHandler;
    private final CodecJackson validationCodec;

    public JobsResource(Spawn spawn, JobRequestHandler requestHandler) {
        this.spawn = spawn;
        this.requestHandler = requestHandler;
        CodecJackson defaultCodec = Jackson.defaultCodec();
        Config defaultConfig = defaultCodec.getGlobalDefaults();
        if (defaultConfig.hasPath("hydra.validation")) {
            Config validationConfig = defaultConfig.getConfig("hydra.validation")
                                                   .withFallback(defaultConfig)
                                                   .resolve();
            validationCodec = defaultCodec.withConfig(validationConfig);
        } else {
            validationCodec = defaultCodec;
        }
    }

    @POST
    @Path("/permissions")
    @Produces(MediaType.APPLICATION_JSON)
    public Response changePermissions(@FormParam("jobs") String jobarg,
                                      @FormParam("owner") String owner,
                                      @FormParam("group") String group,
                                      @FormParam("ownerWritable") String ownerWritable,
                                      @FormParam("groupWritable") String groupWritable,
                                      @FormParam("worldWritable") String worldWritable,
                                      @FormParam("ownerExecutable") String ownerExecutable,
                                      @FormParam("groupExecutable") String groupExecutable,
                                      @FormParam("worldExecutable") String worldExecutable,
                                      @FormParam("user") String user,
                                      @FormParam("token") String token,
                                      @FormParam("sudo") String sudo) {
        if (jobarg == null) {
            return Response.status(Response.Status.BAD_REQUEST).entity("Missing 'jobs' parameter").build();
        }
        Response response = validateChangePermissions("ownerWritable", ownerWritable);
        if (response != null) {
            return response;
        }
        response = validateChangePermissions("groupWritable", groupWritable);
        if (response != null) {
            return response;
        }
        response = validateChangePermissions("worldWritable", worldWritable);
        if (response != null) {
            return response;
        }
        response = validateChangePermissions("ownerExecutable", ownerExecutable);
        if (response != null) {
            return response;
        }
        response = validateChangePermissions("groupExecutable", groupExecutable);
        if (response != null) {
            return response;
        }
        response = validateChangePermissions("worldExecutable", worldExecutable);
        if (response != null) {
            return response;
        }
        List<String> jobIds = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(jobarg);
        List<String> changed = new ArrayList<>();
        List<String> unchanged = new ArrayList<>();
        List<String> notFound = new ArrayList<>();
        List<String> notPermitted = new ArrayList<>();
        try {
            for (String jobId : jobIds) {
                Job job = spawn.getJob(jobId);
                if (job == null) {
                    notFound.add(jobId);
                } else if (!spawn.getPermissionsManager().canModifyPermissions(user, token, sudo, job)) {
                    notPermitted.add(jobId);
                } else {
                    boolean modified = false;
                    if (LessStrings.isNotEmpty(owner) && !owner.equals(job.getOwner())) {
                        job.setOwner(owner);
                        modified = true;
                    }
                    if (LessStrings.isNotEmpty(group) && !group.equals(job.getGroup())) {
                        job.setGroup(group);
                        modified = true;
                    }
                    if (MODIFYING_PERMISSIONS.contains(ownerWritable)) {
                        boolean newValue = Boolean.valueOf(ownerWritable);
                        if (job.isOwnerWritable() != newValue) {
                            job.setOwnerWritable(newValue);
                            modified = true;
                        }
                    }
                    if (MODIFYING_PERMISSIONS.contains(groupWritable)) {
                        boolean newValue = Boolean.valueOf(groupWritable);
                        if (job.isGroupWritable() != newValue) {
                            job.setGroupWritable(newValue);
                            modified = true;
                        }
                    }
                    if (MODIFYING_PERMISSIONS.contains(worldWritable)) {
                        boolean newValue = Boolean.valueOf(worldWritable);
                        if (job.isWorldWritable() != newValue) {
                            job.setWorldWritable(newValue);
                            modified = true;
                        }
                    }
                    if (MODIFYING_PERMISSIONS.contains(ownerExecutable)) {
                        boolean newValue = Boolean.valueOf(ownerExecutable);
                        if (job.isOwnerExecutable() != newValue) {
                            job.setOwnerExecutable(newValue);
                            modified = true;
                        }
                    }
                    if (MODIFYING_PERMISSIONS.contains(groupExecutable)) {
                        boolean newValue = Boolean.valueOf(groupExecutable);
                        if (job.isGroupExecutable() != newValue) {
                            job.setGroupExecutable(newValue);
                            modified = true;
                        }
                    }
                    if (MODIFYING_PERMISSIONS.contains(worldExecutable)) {
                        boolean newValue = Boolean.valueOf(worldExecutable);
                        if (job.isWorldExecutable() != newValue) {
                            job.setWorldExecutable(newValue);
                            modified = true;
                        }
                    }
                    if (modified) {
                        spawn.updateJob(job);
                        changed.add(jobId);
                    } else {
                        unchanged.add(jobId);
                    }
                }
            }
        } catch (Exception e) {
            return buildServerError(e);
        }
        try {
            String json = CodecJSON.encodeString(ImmutableMap.of(
                    "changed", changed,
                    "unchanged", unchanged,
                    "notFound", notFound,
                    "notPermitted", notPermitted));
            return Response.ok(json).build();
        } catch (JsonProcessingException e) {
            return buildServerError(e);
        }
    }

    private Response validateChangePermissions(String parameterName, String parameterValue) {
        if ((parameterValue != null) && !PERMISSIONS.contains(parameterValue)) {
            return Response.status(Response.Status.BAD_REQUEST)
                           .entity(parameterName + " must be one of: " + PERMISSIONS)
                           .build();
        } else {
            return null;
        }
    }

    @GET
    @Path("/enable")
    @Produces(MediaType.APPLICATION_JSON)
    public Response enableJob(@QueryParam("jobs") String jobarg,
                              @QueryParam("enable") @DefaultValue("1") String enableParam,
                              @QueryParam("unsafe") @DefaultValue("false") boolean unsafe,
                              @QueryParam("user") String user,
                              @QueryParam("token") String token,
                              @QueryParam("sudo") String sudo) {
        boolean enable = enableParam.equals("1");
        if (jobarg == null) {
            return Response.status(Response.Status.BAD_REQUEST).entity("Missing 'jobs' parameter").build();
        }
        List<String> jobIds = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(jobarg);
        String action = enable ? (unsafe ? "unsafely enable" : "enable") : "disable";
        emitLogLineForAction(user, action + " jobs " + jobarg);

        List<String> changed = new ArrayList<>();
        List<String> unchanged = new ArrayList<>();
        List<String> notFound = new ArrayList<>();
        List<String> notAllowed = new ArrayList<>();
        List<String> notPermitted = new ArrayList<>();

        try {
            for (String jobId : jobIds) {
                Job job = spawn.getJob(jobId);
                if (job == null) {
                    notFound.add(jobId);
                } else if (!spawn.getPermissionsManager().isExecutable(user, token, sudo, job)) {
                    notPermitted.add(jobId);
                } else if (enable && !unsafe && job.getState() != JobState.IDLE) {
                    // request to enable safely, so do not allow if job is not IDLE
                    notAllowed.add(jobId);
                } else if (job.setEnabled(enable)) {
                    spawn.updateJob(job);
                    changed.add(jobId);
                } else {
                    unchanged.add(jobId);
                }
            }
            log.info("{} jobs: changed={}, unchanged={}, not found={}, not permitted={}, cannot safely enable={}",
                     action, changed, unchanged, notFound, notPermitted, notAllowed);
        } catch (Exception e) {
            return buildServerError(e);
        }

        try {
            String json = CodecJSON.encodeString(ImmutableMap.of(
                    "changed", changed,
                    "unchanged", unchanged,
                    "notFound", notFound,
                    "notAllowed", notAllowed,
                    "notPermitted", notPermitted));
            return Response.ok(json).build();
        } catch (JsonProcessingException e) {
            return buildServerError(e);
        }
    }

    @GET
    @Path("/rebalance")
    @Produces(MediaType.TEXT_PLAIN)
    public Response rebalanceJob(@QueryParam("id") String id,
                                 @QueryParam("user") String user,
                                 @QueryParam("token") String token,
                                 @QueryParam("sudo") String sudo,
                                 @QueryParam("tasksToMove") @DefaultValue("-1") Integer tasksToMove) {
        emitLogLineForAction(user, "job rebalance on " + id + " tasksToMove=" + tasksToMove);
        try {
            RebalanceOutcome ro = spawn.rebalanceJob(id, tasksToMove, user, token, sudo);
            String outcome = ro.toString();
            return Response.ok(outcome).build();
        } catch (Exception ex) {
            log.warn("", ex);
            return Response.serverError().entity("Rebalance Error: " + ex.getMessage()).build();
        }
    }

    /**
     * expand the job's macros and send the text to the user
     */
    @GET
    @Path("/expand")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response expandJobGet(@QueryParam("id") @DefaultValue("") String id,
                                 @QueryParam("format") String format) {
        if (id.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                           .header("topic", "Expansion Error")
                           .entity("{error:'unable to expand job, job id must be non null and not empty'}")
                           .build();
        } else {
            try {
                String expandedJobConfig = spawn.expandJob(id);
                return formatConfig(format, expandedJobConfig);
            } catch (Exception ex) {
                return buildServerError(ex);
            }
        }
    }

    @POST
    @Path("/expand")
    @Produces(MediaType.APPLICATION_JSON)
    public Response expandJobPost(@QueryParam("pairs") KVPairs kv) throws Exception {
        try {
            String format = kv.takeValue("format");
            String expandedConfig = configExpansion(kv);
            return formatConfig(format, expandedConfig);
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    private Response formatConfig(String format, String configBody) {
        if (format == null) {
            return Response.ok("attachment; filename=expanded_job.json", MediaType.APPLICATION_OCTET_STREAM)
                           .entity(configBody)
                           .header("topic", "expanded_job")
                           .build();
        }
        String normalizedFormat = format.toLowerCase();
        String formattedConfig;
        Config config = ConfigFactory.parseString(
                configBody, ConfigParseOptions.defaults().setOriginDescription("job.conf"));
        Config jobConfig = config;
        PluginRegistry pluginRegistry;
        if (config.hasPath("global")) {
            Config globalDefaults = config.getConfig("global")
                                          .withFallback(ConfigFactory.load())
                                          .resolve();
            pluginRegistry = new PluginRegistry(globalDefaults);
            jobConfig = config.withoutPath("global");
        } else {
            pluginRegistry = PluginRegistry.defaultRegistry();
        }
        jobConfig = jobConfig.resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                             .resolveWith(pluginRegistry.config());
        ConfigValue expandedConfig = Configs.expandSugar(TaskRunnable.class, jobConfig.root(), pluginRegistry);
        switch (normalizedFormat) {
            // auto json/hocon + json output
            case "json":
                formattedConfig = expandedConfig.render(ConfigRenderOptions.concise().setFormatted(true));
                return Response.ok("attachment; filename=expanded_job.json", MediaType.APPLICATION_JSON)
                               .entity(formattedConfig)
                               .header("topic", "expanded_job")
                               .build();
            case "hocon":
                // hocon parse + non-json output
                formattedConfig = expandedConfig.render(ConfigRenderOptions.defaults());
                return Response.ok("attachment; filename=expanded_job.json", MediaType.APPLICATION_OCTET_STREAM)
                               .entity(formattedConfig)
                               .header("topic", "expanded_job")
                               .build();
            default:
                throw new IllegalArgumentException("invalid config format specified: " + normalizedFormat);
        }
    }

    private String configExpansion(KVPairs kv) throws TokenReplacerOverflowException {
        KVPair idPair = kv.removePair("id");
        String id = (idPair == null) ? null : idPair.getValue();
        String jobConfig = kv.removePair("config").getValue();

        String expandedConfig = JobExpand.macroExpand(spawn, jobConfig);
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

    /** url called via ajax by client to rebalance a job */
    @GET
    @Path("/synchronize")
    @Produces(MediaType.APPLICATION_JSON)
    public Response synchronizeJob(@QueryParam("id") @DefaultValue("") String id,
                                   @QueryParam("user") String user,
                                   @QueryParam("token") String token,
                                   @QueryParam("sudo") String sudo) {
        emitLogLineForAction(user, "job synchronize on " + id);
        return spawn.synchronizeJobState(id, user, token, sudo);
    }

    @GET
    @Path("/delete") //TODO: should this be a @delete?
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteJob(@QueryParam("id") @DefaultValue("") String id,
                              @QueryParam("user") String user,
                              @QueryParam("token") String token,
                              @QueryParam("sudo") String sudo) {
        Job job = spawn.getJob(id);
        if (job == null) {
            return Response.serverError().entity("Job with id " + id + " cannot be found").build();
        } else if (!spawn.getPermissionsManager().isWritable(user, token, sudo, job)) {
            return Response.serverError().entity("Insufficient privileges to delete job " + id).build();
        } else if (job.getCountActiveTasks() != 0) {
            return Response.serverError().entity("A job with active tasks cannot be deleted").build();
        } else {
            emitLogLineForAction(user, "job delete on " + id);
            try {
                DeleteStatus status = spawn.deleteJob(id);
                switch (status) {
                    case SUCCESS:
                        return Response.ok().build();
                    case JOB_MISSING:
                        log.warn("[job.delete] {} missing job", id);
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
                JSONObject jobUpdateEvent = Spawn.getJobUpdateEvent(job);
                jobs.put(jobUpdateEvent);
            }
            return Response.ok(jobs.toString()).build();
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    private static JSONObject dependencyGraphNode(@Nonnull IJob job) throws Exception {
        JSONObject newNode = job.toJSON();
        newNode.remove("config");
        newNode.remove("nodes");
        return newNode;
    }

    private static JSONObject dependencyGraphEdge(@Nonnull String source, @Nonnull String sink)
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
    public Response toggleJobAlerts(@QueryParam("enable") @DefaultValue("true") Boolean enable,
                                    @QueryParam("user") String user,
                                    @QueryParam("token") String token,
                                    @QueryParam("sudo") String sudo) {
        try {
            if (!spawn.getPermissionsManager().adminAction(user, token, sudo)) {
                return Response.ok("false").build();
            } else if (enable) {
                spawn.getJobAlertManager().enableAlerts();
            } else {
                spawn.getJobAlertManager().disableAlerts();
            }
        } catch (Exception e) {
            log.warn("Failed to toggle alerts", e);
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
                    Object fieldObject =
                            "config".equals(field.get()) ? spawn.getJobConfig(id) : jobobj.get(field.get());
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
    @Produces(MediaType.APPLICATION_JSON)
    public Response saveJob(@QueryParam("pairs") KVPairs kv,
                            @QueryParam("user") String user,
                            @QueryParam("token") String token,
                            @QueryParam("sudo") String sudo) {
        String id = KVUtils.getValue(kv, "", "id", "job");
        try {
            Job job = requestHandler.createOrUpdateJob(kv, user, token, sudo);
            log.info("[job/save][user={}][id={}] Job {}", user, job.getId(), jobUpdateAction(id));
            return Response.ok("{\"id\":\"" + job.getId() + "\",\"updated\":\"true\"}").build();
        } catch (IllegalArgumentException e) {
            log.warn("[job/save][user={}][id={}] Bad parameter: {}", user, id, e.getMessage(), e);
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        } catch (InsufficientPrivilegesException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity(e.getMessage()).build();
        } catch (Exception e) {
            log.error("[job/save][user={}][id={}] Internal error: {}", user, id, e.getMessage(), e);
            return buildServerError(e);
        }
    }
    
    private String jobUpdateAction(String id) {
        return LessStrings.isEmpty(id) ? "created" : "updated";
    }

    /**
     * Creates or updates a job, and optionally kicks it or its tasks. (THIS IS A LEGACY METHOD!)
     * 
     * The functionality of this end point is a legacy from spawn v1's job.submit end point (where
     * one specifies spawn=1 to kick a job). Spawn v2 uses the /job/save end point to create or
     * update a job, and /job/start to kick a job to separate the logic. We keep this end point to
     * avoid breaking anything, but consider this end point deprecated.
     */
    @POST
    @Path("/submit")
    @Produces(MediaType.APPLICATION_JSON)
    public Response submitJob(@QueryParam("pairs") KVPairs kv,
                              @QueryParam("user") String user,
                              @QueryParam("token") String token,
                              @QueryParam("sudo") String sudo) {
        String id = KVUtils.getValue(kv, "", "id", "job");
        log.warn("[job/submit][user={}][id={}] This end point is deprecated", user, id);
        try {
            Job job = requestHandler.createOrUpdateJob(kv, user, token, sudo);
            // optionally kicks the job/task
            requestHandler.maybeKickJobOrTask(kv, job);
            log.info("[job/submit][user={}][id={}] Job {}", user, job.getId(), jobUpdateAction(id));
            return Response.ok("{\"id\":\"" + job.getId() + "\",\"updated\":\"true\"}").build();
        } catch (IllegalArgumentException e) {
            log.warn("[job/submit][user={}][id={}] Bad parameter: {}", user, id, e.getMessage(), e);
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        } catch (InsufficientPrivilegesException e) {
            log.warn("[job/submit][user={}][id={}] Privileges error: {}", user, id, e.getMessage(), e);
            return Response.status(Response.Status.UNAUTHORIZED).entity(e.getMessage()).build();
        } catch (Exception e) {
            log.error("[job/submit][user={}][id={}] Internal error: {}", user, id, e.getMessage(), e);
            return buildServerError(e);
        }
    }


    @GET
    @Path("/revert")
    @Produces(MediaType.APPLICATION_JSON)
    public Response revertJob(@QueryParam("id") String id,
                              @QueryParam("type") @DefaultValue("gold") String type,
                              @QueryParam("revision") @DefaultValue("-1") Integer revision,
                              @QueryParam("node") @DefaultValue("-1") Integer node,
                              @QueryParam("time") @DefaultValue("-1") Long time,
                              @QueryParam("user") String user,
                              @QueryParam("token") String token,
                              @QueryParam("sudo") String sudo) {
        try {
            emitLogLineForAction(user, "job revert on " + id + " of type " + type);
            IJob job = spawn.getJob(id);
            boolean success = spawn.revertJobOrTask(job.getId(), user, token, sudo, node, type, revision, time);
            if (success) {
                return Response.ok("{\"id\":\"" + job.getId() + "\", \"action\":\"reverted\"}").build();
            } else {
                return Response.status(Response.Status.UNAUTHORIZED).entity(
                        "{\"id\":\"" + job.getId() + "\", \"action\":\"unauthorized\"}").build();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return Response.serverError().entity(ex).build();
        }
    }

    @GET
    @Path("/backups.list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRevisions(@QueryParam("id") String id,
                                 @QueryParam("node") @DefaultValue("-1") int node,
                                 @QueryParam("user") String user,
                                 @QueryParam("token") String token) {
        try {
            if (spawn.isSpawnMeshAvailable()) {
                IJob job = spawn.getJob(id);
                Map<ScheduledBackupType, SortedSet<Long>> backupDates = spawn.getJobBackups(job.getId(), node);
                String jsonString = MAPPER.writeValueAsString(backupDates);
                return Response.ok(jsonString).build();
            } else {
                return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                               .entity("Spawn Mesh is not available.")
                               .build();
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
                    HostState host = spawn.hostManager.getHostState(task.getHostUUID());
                    JSONObject json = task.toJSON();
                    json.put("host", host.getHost());
                    json.put("hostPort", host.getPort());

                    JSONArray taskReplicas = new JSONArray();
                    for (JobTaskReplica replica : task.getAllReplicas()) {
                        JSONObject replicaJson = new JSONObject();
                        HostState replicaHost = spawn.hostManager.getHostState(replica.getHostUUID());
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

    private void startJobHelper(String jobId, int taskId, int priority) throws Exception {
        if (taskId < 0) {
            spawn.startJob(jobId, priority);
        } else {
            spawn.startTask(jobId, taskId, true, priority, false);
        }
    }

    /**
     * Support for both the undocumented API that uses "jobid" and "select"
     * and the documented API that uses "id" and "task".
     */
    @GET
    @Path("/start")
    @Produces(MediaType.APPLICATION_JSON)
    public Response startJob(@QueryParam("jobid") Optional<String> jobIds,
                              @QueryParam("select") @DefaultValue("-1") int select,
                              @QueryParam("id") Optional<String> id,
                              @QueryParam("task") @DefaultValue("-1") int task,
                              @QueryParam("priority") @DefaultValue("0") int priority,
                              @QueryParam("user") String user,
                              @QueryParam("token") String token,
                              @QueryParam("sudo") String sudo) {

        List<String> success = new ArrayList<>();
        List<String> error = new ArrayList<>();
        List<String> unauthorized = new ArrayList<>();
        try {
            if (jobIds.isPresent()) {
                String[] joblist = LessStrings.splitArray(jobIds.get(), ",");
                for (String aJob : joblist) {
                    IJob job = spawn.getJob(aJob);
                    if (job == null) {
                        error.add(aJob);
                    } else if (!spawn.getPermissionsManager().isExecutable(user, token, sudo, job)) {
                        unauthorized.add(aJob);
                    } else {
                        startJobHelper(aJob, select, priority);
                        success.add(aJob);
                    }
                }
            } else if (id.isPresent()) {
                String jobId = id.get();
                IJob job = spawn.getJob(jobId);
                if (job == null) {
                    error.add(jobId);
                } else if (!spawn.getPermissionsManager().isExecutable(user, token, sudo, job)) {
                    unauthorized.add(jobId);
                } else {
                    startJobHelper(jobId, select, priority);
                    success.add(jobId);
                }
            } else {
                return Response.status(Response.Status.BAD_REQUEST).entity("job id not specified").build();
            }
            String json = CodecJSON.encodeString(ImmutableMap.of(
                    "success", success,
                    "error", error,
                    "unauthorized", unauthorized));
            return Response.ok(json).build();
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
    public Response fixJobDirs(@QueryParam("id") String id, @QueryParam("node") @DefaultValue("-1") int node,
                               @QueryParam("user") String user,  @QueryParam("token") String token,
                               @QueryParam("sudo") String sudo) {
        try {
            IJob job = spawn.getJob(id);
            if (job != null) {
                if (!spawn.getPermissionsManager().isExecutable(user, token, sudo, job)) {
                    return Response.status(Response.Status.UNAUTHORIZED).entity(
                            "Insufficient privileges to fix directories for job " + id).build();
                } else {
                    return Response.ok(spawn.fixTaskDir(id, node, false, false).toString()).build();
                }
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

    @GET
    @Path("/config.deleted")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getDeletedJobConfig(@QueryParam("id") String id) {
        Response.ResponseBuilder rb;
        if (spawn.getJob(id) == null) {
            try {
                String jobConfig = spawn.getDeletedJobConfig(id);
                if (jobConfig != null) {
                    rb = Response.ok(jobConfig);
                } else {
                    String err = "Unable to find commit history for job " + id;
                    rb = Response.status(Response.Status.BAD_REQUEST).entity(err);
                }
            } catch (Exception e) {
                rb = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage());
            }
        } else {
            rb = Response.status(Response.Status.BAD_REQUEST).entity("Job " + id + " exists!");
        }
        return rb.build();
    }

    
    private static Response validateCreateError(String message, JSONArray lines,
                                                JSONArray columns, String errorType) throws JSONException {
        JSONObject error = new JSONObject();
        error.put("message", message).put("lines", lines).put("columns", columns).put("result", errorType);
        return Response.ok().entity(error.toString()).build();
    }

    private Response validateJobConfig(String expandedConfig) throws JSONException {
        String message = null;
        int lineNumber = 1;
        try {
            TaskRunner.makeTask(expandedConfig, validationCodec);
            return Response.ok(new JSONObject().put("result", "valid").toString()).build();
        } catch (ConfigException ex) {
            ConfigOrigin exceptionOrigin = ex.origin();
            message = ex.getMessage();
            if (exceptionOrigin != null) {
                lineNumber = exceptionOrigin.lineNumber();
            }
        } catch (JsonProcessingException ex) {
            JsonLocation jsonLocation = ex.getLocation();
            if (jsonLocation != null) {
                lineNumber = jsonLocation.getLineNr();
                message = "Line: " + lineNumber + " ";
            }
            message += ex.getOriginalMessage();
            if (ex instanceof JsonMappingException) {
                String pathReference = ((JsonMappingException) ex).getPathReference();
                if (pathReference != null) {
                    message += " referenced via: " + pathReference;
                }
            }
        } catch (Exception other) {
            message = other.toString();
        }
        JSONArray lineColumns = new JSONArray();
        JSONArray lineErrors = new JSONArray();
        lineColumns.put(1);
        lineErrors.put(lineNumber);
        return validateCreateError(message, lineErrors, lineColumns, "postExpansionError");
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
                    return validateJobConfig(expandedConfig);
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
            return validateJobConfig(expandedConfig);
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

    void stopJobHelper(IJob job, Optional<Boolean> cancelParam,
                          Optional<Boolean> forceParam,
                          int nodeId) throws Exception {
        String id = job.getId();
        boolean cancelRekick = cancelParam.or(false);
        boolean force = forceParam.or(false);
        // cancel re-spawning
        if (cancelRekick) {
            job.setRekickTimeout(null);
        }
        log.warn("[job.stop] {}/{}, cancel={}, force={}", job.getId(), nodeId, cancelRekick, force);
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
                            @QueryParam("task") @DefaultValue("-1") int task,
                            @QueryParam("user") String user,
                            @QueryParam("token") String token,
                            @QueryParam("sudo") String sudo) {
        List<String> success = new ArrayList<>();
        List<String> error = new ArrayList<>();
        List<String> unauthorized = new ArrayList<>();
        try {
            if (jobIds.isPresent()) {
                String ids = jobIds.get();
                String[] joblist = LessStrings.splitArray(ids, ",");
                for (String jobName : joblist) {
                    IJob job = spawn.getJob(jobName);
                    if (job == null) {
                        error.add(jobName);
                    } else if (!spawn.getPermissionsManager().isExecutable(user, token, sudo, job)) {
                        unauthorized.add(jobName);
                    } else {
                        stopJobHelper(job, cancelParam, forceParam, nodeParam);
                        success.add(jobName);
                    }
                }
            } else if (id.isPresent()) {
                String jobId = id.get();
                IJob job = spawn.getJob(jobId);
                if (job == null) {
                    error.add(jobId);
                } else if (!spawn.getPermissionsManager().isExecutable(user, token, sudo, job)) {
                    unauthorized.add(jobId);
                } else {
                    stopJobHelper(job, cancelParam, forceParam, nodeParam);
                    success.add(jobId);
                }
            } else {
                return Response.status(Response.Status.BAD_REQUEST).entity("job id not specified").build();
            }
            String json = CodecJSON.encodeString(ImmutableMap.of(
                    "success", success,
                    "error", error,
                    "unauthorized", unauthorized));
            return Response.ok(json).build();
        } catch (Exception ex) {
            return buildServerError(ex);
        }
    }

    @GET
    @Path("/saveAllJobs")
    @Produces(MediaType.APPLICATION_JSON)
    public Response saveAllJobs(@QueryParam("user") String user,  @QueryParam("token") String token,
                                @QueryParam("sudo") String sudo) {
        // Primarily for use in emergencies where updates have not been sent to the data store for a while
        try {
            if (!spawn.getPermissionsManager().adminAction(user, token, sudo)) {
                return Response.ok("{\"operation\":\"failed: insufficient priviledges\"").build();
            } else {
                spawn.saveAllJobs();
                return Response.ok(("{\"operation\":\"sucess\"")).build();
            }
        } catch (Exception ex) {
            log.trace("Save all jobs exception", ex);
            return Response.ok("{\"operation\":\"failed: " + ex.toString() + "\"").build();
        }

    }

    private static Response buildServerError(Exception exception) {
        log.warn("", exception);
        String message = exception.getMessage();
        if (message == null) {
            message = exception.toString();
        }
        return Response.serverError().entity(message).build();
    }

    private static void emitLogLineForAction(String user, String desc) {
        log.warn("User {} initiated action: {}", user, desc);
    }
}
