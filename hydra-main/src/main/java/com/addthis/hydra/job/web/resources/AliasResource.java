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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

import com.addthis.basis.kv.KVPairs;

import com.addthis.hydra.job.alias.AliasManager;
import com.addthis.hydra.job.web.jersey.User;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import com.yammer.dropwizard.auth.Auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/alias")
public class AliasResource {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AliasResource.class);

    private final AliasManager aliasManager;

    public AliasResource(AliasManager aliasManager) {
        this.aliasManager = aliasManager;
    }

    @GET
    @Path("/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listAliases(@QueryParam("id") String id) {
        try {
            JSONArray aliases = new JSONArray();
            for (Map.Entry<String, List<String>> alias : aliasManager.getAliases().entrySet()) {
                JSONObject aliasJson = new JSONObject();
                JSONArray aliasJobs = new JSONArray();
                for (String key : alias.getValue()) {
                    aliasJobs.put(key);
                }
                aliases.put(aliasJson.put("name", alias.getKey()).put("jobs", aliasJobs));
            }
            return Response.ok().entity(aliases.toString()).build();
        } catch (Exception ex) {
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }

    @GET
    @Path("/map")
    @Produces(MediaType.APPLICATION_JSON)
    public Response mapAliases(@QueryParam("id") String id) {
        try {
            JSONObject aliases = new JSONObject();
            for (Map.Entry<String, List<String>> alias : aliasManager.getAliases().entrySet()) {
                JSONArray aliasJobs = new JSONArray();
                for (String key : alias.getValue()) {
                    aliasJobs.put(key);
                }
                aliases.put(alias.getKey(), aliasJobs);
            }
            return Response.ok().entity(aliases.toString()).build();
        } catch (Exception ex) {
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }

    @GET
    @Path("/get")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAlias(@QueryParam("id") String id) {
        try {
            List<String> alias = aliasManager.getAliases().get(id);
            JSONObject aliasJson = new JSONObject();
            JSONArray aliasJobs = new JSONArray();
            for (String key : alias) {
                aliasJobs.put(key);
            }
            return Response.ok().entity(aliasJson.put("name", id).put("jobs", aliasJobs).toString()).build();
        } catch (Exception ex) {
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }

    @POST
    @Path("/save")
    @Produces(MediaType.APPLICATION_JSON)
    public Response postAlias(@QueryParam("pairs") KVPairs kv, @Auth User user) {
        if (!kv.hasKey("name") || !kv.hasKey("jobs")) {
            return Response.serverError().entity("must supply alias name and jobs").build();
        }
        try {
            List<String> jobs = Lists.newArrayList(Splitter.on(',').split(kv.getValue("jobs")));
            aliasManager.addAlias(kv.getValue("name"), jobs);
            log.info("user action from {}: saving alias {}", user, kv);
            return Response.ok().entity(new JSONObject().put("name", kv.getValue("name")).put("jobs", new JSONArray(jobs)).toString()).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e.toString()).build();
        }
    }

    @POST
    @Path("/delete")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteAlias(@QueryParam("pairs") KVPairs kv, @Auth User user) {
        if (!kv.hasKey("name")) {
            return Response.serverError().entity("must supply alias name and jobs").build();
        }
        try {
            aliasManager.deleteAlias(kv.getValue("name"));
            return Response.ok().build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().build();
        }
    }
}
