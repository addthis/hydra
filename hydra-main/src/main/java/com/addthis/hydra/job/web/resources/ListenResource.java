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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.IJob;
import com.addthis.hydra.job.entity.JobCommand;
import com.addthis.hydra.job.entity.JobEntityManager;
import com.addthis.hydra.job.entity.JobMacro;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.job.spawn.ClientEvent;
import com.addthis.hydra.job.spawn.ClientEventListener;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.web.jersey.User;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Optional;

import com.sun.jersey.api.core.HttpContext;
import com.yammer.dropwizard.auth.Auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@Path("/update")
public class ListenResource {

    private static final Logger log = LoggerFactory.getLogger(ListenResource.class);

    private static final int batchInterval = Integer.parseInt(System.getProperty("spawn.batchtime", "500"));
    private static int pollTimeout = Integer.parseInt(System.getProperty("spawn.polltime", "1000"));

    private final Spawn spawn;
    private final SystemResource systemResource;
    private final AtomicInteger clientCounter;
    
    @Context
    private HttpContext context;

    public ListenResource(Spawn spawn, SystemResource systemResource, int pollTimeout) {
        this.spawn = spawn;
        this.systemResource = systemResource;
        this.pollTimeout = pollTimeout;
        clientCounter = new AtomicInteger(0);
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
        ClientEventListener listener = spawn.getClientEventListener(clientId);
        try {
            ClientEvent nextEvent = listener.events.poll(timeout, TimeUnit.MILLISECONDS);
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

    private JSONObject encodeJson(ClientEvent event) throws Exception {
        JSONObject json = event.toJSON();
        return json;
    }

    @GET
    @Path("/setup")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSetup() {
        try {
            JSONObject setup = CodecJSON.encodeJSON(spawn.getSystemManager().getSettings());
            JSONArray jobs = new JSONArray();
            for (IJob job : spawn.listJobsConcurrentImmutable()) {
                JSONObject jobUpdateEvent = Spawn.getJobUpdateEvent(job);
                jobs.put(jobUpdateEvent);
            }
            setup.put("jobs", jobs);
            JSONObject macrolist = new JSONObject();
            JSONObject commandlist = new JSONObject();
            JSONObject hostlist = new JSONObject();
            JSONObject aliases = new JSONObject();
            JobEntityManager<JobMacro> jobMacroManager = spawn.getJobMacroManager();
            JobEntityManager<JobCommand> jobCommandManager = spawn.getJobCommandManager();
            for (String key : jobMacroManager.getKeys()) {
                JobMacro macro = jobMacroManager.getEntity(key);
                macrolist.put(key, macro.toJSON().put("macro", "").put("name", key));
            }
            for (String key : jobCommandManager.getKeys()) {
                JobCommand command = jobCommandManager.getEntity(key);
                commandlist.put(key, command.toJSON().put("name", key));
            }
            for (HostState host : spawn.listHostStatus(null)) {
                hostlist.put(host.getHostUuid(), spawn.getHostStateUpdateEvent(host));
            }
            for (Map.Entry<String, List<String>> alias : spawn.getAliasManager().getAliases().entrySet()) {
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
            setup.put("alerts", spawn.getJobAlertManager().fetchAllAlertsMap());
            setup.put("spawnqueuesize", spawn.getTaskQueuedCount());
            setup.put("clientId", clientCounter.incrementAndGet());
            return Response.ok(setup.toString()).build();
        } catch (Exception ex) {
            ex.printStackTrace();
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }

    /** @deprecated Use {@link SystemResource#quiesceCluster(String, User)} */
    @GET
    @Path("/quiesce")
    @Produces(MediaType.APPLICATION_JSON)
    @Deprecated
    public Response quiesceCluster(@QueryParam("quiesce") String quiesce, @Auth User user) {
        return systemResource.quiesceCluster(quiesce, user);
    }

    /** @deprecated Use {@link SystemResource#getBalanceParams()} */
    @GET
    @Path("/balance.params.get")
    @Produces(MediaType.APPLICATION_JSON)
    @Deprecated
    public Response getBalanceParams() {
        return systemResource.getBalanceParams();
    }

    /** @deprecated Use {@link SystemResource#setBalanceParams(String)} */
    @GET
    @Path("/balance.params.set")
    @Produces(MediaType.APPLICATION_JSON)
    @Deprecated
    public Response setBalanceParams(@QueryParam("params") String params) {
        return systemResource.setBalanceParams(params);
    }

    /** @deprecated Use {@link SystemResource#setObeyTaskLimit(boolean)} */
    @GET
    @Path("/hostfailworker.obeyTaskLimit.set")
    @Produces(MediaType.APPLICATION_JSON)
    @Deprecated
    public Response setObeyTaskLimit(@QueryParam("obey") boolean obey) {
        return systemResource.setObeyTaskLimit(obey);
    }


    /** @deprecated Use {@link SystemResource#getGitProperties()} */
    @GET
    @Path("/git.properties")
    @Produces(MediaType.APPLICATION_JSON)
    @Deprecated
    public Response getGitProperties() {
        return systemResource.getGitProperties();
    }

    /** @deprecated Use {@link SystemResource#datastoreCutover(String, String, int)} */
    @GET
    @Path("/datastore.cutover")
    @Produces(MediaType.APPLICATION_JSON)
    @Deprecated
    public Response datastoreCutover(@QueryParam("src") String src, @QueryParam("tar") String tar, @QueryParam("checkAll") int checkAll) {
        return systemResource.datastoreCutover(src, tar, checkAll);
    }

}
