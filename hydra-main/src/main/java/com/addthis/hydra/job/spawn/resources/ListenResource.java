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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.InputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.codec.CodecJSON;
import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.JobCommand;
import com.addthis.hydra.job.JobMacro;
import com.addthis.hydra.job.Spawn;
import com.addthis.hydra.job.SpawnBalancerConfig;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.entities.TopicEvent;
import com.addthis.hydra.job.spawn.jersey.User;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Optional;

import com.sun.jersey.api.core.HttpContext;
import com.yammer.dropwizard.auth.Auth;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
@Path("/update")
public class ListenResource {

    private static Logger log = LoggerFactory.getLogger(ListenResource.class);

    private final Spawn spawn;
    private static int batchInterval = Integer.parseInt(System.getProperty("spawn.batchtime", "500"));
    private static int pollTimeout = Integer.parseInt(System.getProperty("spawn.polltime", "1000"));

    private final Properties gitProperties;

    @Context
    private HttpContext context;

    private AtomicInteger clientCounter;

    private static final CodecJSON codec = new CodecJSON(true);

    public ListenResource(Spawn spawn, int pollTimeout) {
        this.spawn = spawn;
        this.pollTimeout = pollTimeout;
        clientCounter = new AtomicInteger(0);
        gitProperties = new Properties();
        try {
            InputStream in = getClass().getResourceAsStream("/hydra-git.properties");
            gitProperties.load(in);
            in.close();
        } catch (Exception ex) {
            //ex.printStackTrace();
            log.warn("Error loading git.properties, possibly jar was not compiled with maven.");
        }
    }

    @GET
    @Path("/batch")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getListenBatch(@QueryParam("timeout") Optional<Integer> timeoutParameter,
            @QueryParam("batchtime") Optional<Integer> batchtimeParameter,
            @QueryParam("clientId") Optional<String> clientIdParameter) {
        Response response;
        String clientId = (clientIdParameter.isPresent() ? clientIdParameter.get() : "noid");
        int timeout = timeoutParameter.or(pollTimeout);
        int batchTime = batchtimeParameter.or(batchInterval);
        Spawn.ClientEventListener listener = spawn.getClientEventListener(clientId);
        List<TopicEvent> events = new ArrayList<TopicEvent>();
        try {
            Spawn.ClientEvent nextEvent = listener.events.poll(timeout, TimeUnit.MILLISECONDS);
            if (nextEvent != null) {
                long mark = System.currentTimeMillis();
                JSONArray payload = new JSONArray();
                payload.put(encodeJson(nextEvent));
                for (int i = 50; i > 0; i--) {
                    nextEvent = listener.events.poll(batchTime, TimeUnit.MILLISECONDS);
                    if (nextEvent != null) {
                        JSONObject json = encodeJson(nextEvent);
                        payload.put(json);
                    }
                    if (System.currentTimeMillis() - mark > batchTime) {
                        break;
                    }
                }
                response = Response.ok(payload.toString()).build();
            } else {
                response = Response.notModified().build();
            }
        } catch (InterruptedException ex) {
            response = Response.notModified().build();
        } catch (Exception ex)  {
            log.warn("", ex);
            response = Response.serverError().build();
        }
        return response;
    }

    @GET
    @Path("/setup")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSetup() {
        try {
            JSONObject setup = new JSONObject();
            JSONArray jobs = new JSONArray();
            for (IJob job : spawn.listJobs()) {
                JSONObject jobUpdateEvent = spawn.getJobUpdateEvent(job);
                jobs.put(jobUpdateEvent);
            }
            setup.put("jobs", jobs);
            JSONObject macrolist = new JSONObject();
            JSONObject commandlist = new JSONObject();
            JSONObject hostlist = new JSONObject();
            JSONObject aliases = new JSONObject();
            for (String key : spawn.listMacros()) {
                JobMacro macro = spawn.getMacro(key);
                macrolist.put(key, macro.toJSON().put("macro", "").put("name", key));
            }
            for (String key : spawn.listCommands()) {
                JobCommand command = spawn.getCommand(key);
                commandlist.put(key, command.toJSON().put("name", key));
            }
            for (HostState host : spawn.listHostStatus(null)) {
                hostlist.put(host.getHostUuid(), spawn.getHostStateUpdateEvent(host));
            }
            for (Map.Entry<String, List<String>> alias : spawn.getAliases().entrySet()) {
                JSONObject aliasJson = new JSONObject();
                JSONArray aliasJobs = new JSONArray();
                for (String key : alias.getValue()) {
                    aliasJobs.put(key);
                }
                aliases.put(alias.getKey(), aliasJson.put("name", alias.getKey()).put("jobs", aliasJobs));
            }
            setup.put("macros", macrolist);
            setup.put("commands", commandlist);
            setup.put("hosts", hostlist);
            setup.put("aliases", aliases);
            setup.put("alerts", spawn.fetchAllAlertsMap());
            setup.put("quiesced", (spawn.getSettings().getQuiesced() ? "1" : "0"));
            int numQueued = spawn.getTaskQueuedCount();
            setup.put("spawnqueuesize", numQueued);
            setup.put("clientId", clientCounter.incrementAndGet());
            return Response.ok(setup.toString()).build();
        } catch (Exception ex) {
            ex.printStackTrace();
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }

    @GET
    @Path("/quiesce")
    @Produces(MediaType.APPLICATION_JSON)
    public Response quiesceCluster(@QueryParam("quiesce") String quiesce, @Auth User user) {
        try {
            if (user.getAdmin()) {
                spawn.getSettings().setQuiesced(quiesce.equals("1"));
                spawn.sendClusterQuiesceEvent(user.getUsername());
                return Response.ok(new JSONObject().put("quiesced", (spawn.getSettings().getQuiesced() ? "1" : "0")).toString()).build();
            } else {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
        } catch (Exception ex) {
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/balance.params.get")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getBalanceParams() {
        try {
            return Response.ok(codec.encode(spawn.getSpawnBalancer().getConfig())).build();
        } catch (Exception e) {
            return Response.serverError().entity("Error getting balance parameters.").build();
        }
    }

    @GET
    @Path("/balance.params.set")
    @Produces(MediaType.APPLICATION_JSON)
    public Response setBalanceParams(@QueryParam("params") String params) {
        try {
            SpawnBalancerConfig config = new SpawnBalancerConfig();
            codec.decode(config, params.getBytes());
            spawn.updateSpawnBalancerConfig(config);
            spawn.writeSpawnBalancerConfig();
            return Response.ok().build();
        } catch (Exception e) {
            log.warn("Failed to set SpawnBalanceConfig: " + e, e);
            return Response.serverError().entity("Error getting balance parameters.").build();
        }
    }


    @GET
    @Path("/git.properties")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getGitProperties() {
        try {
            if (gitProperties.size() > 0) {
                JSONObject prop = new JSONObject();
                prop.put("commitIdAbbrev", gitProperties.get("git.commit.id.abbrev"));
                prop.put("commitUserEmail", gitProperties.get("git.commit.user.email"));
                prop.put("commitMessageFull", gitProperties.get("git.commit.message.full"));
                prop.put("commitId", gitProperties.get("git.commit.id"));
                prop.put("commitUserName", gitProperties.get("git.commit.user.name"));
                prop.put("buildUserName", gitProperties.get("git.build.user.name"));
                prop.put("commitIdDescribe", gitProperties.get("git.commit.id.describe"));
                prop.put("buildUserEmail", gitProperties.get("git.build.user.email"));
                prop.put("branch", gitProperties.get("git.branch"));
                prop.put("commitTime", gitProperties.get("git.commit.time"));
                prop.put("buildTime", gitProperties.get("git.build.time"));
                return Response.ok(prop.toString()).build();
            } else {
                return Response.serverError().entity("Error loading git properties file.").build();
            }
        } catch (Exception ex) {
            //ex.printStackTrace();
            return Response.serverError().entity("Error loading git properties file. This is possibly because maven git plugin was not used for build.").build();
        }
    }

    private JSONObject encodeJson(Spawn.ClientEvent event) throws Exception {
        JSONObject json = event.toJSON();
        return json;
    }
}
