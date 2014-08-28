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

import com.addthis.basis.kv.KVPairs;

import com.addthis.hydra.job.entity.JobEntityManager;
import com.addthis.hydra.job.entity.JobMacro;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.web.jersey.User;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import com.yammer.dropwizard.auth.Auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/macro")
public class MacroResource {

    private static Logger log = LoggerFactory.getLogger(MacroResource.class);

    private final JobEntityManager<JobMacro> jobMacroManager;

    public MacroResource(JobEntityManager<JobMacro> jobMacroManager) {
        this.jobMacroManager = jobMacroManager;
    }

    @GET
    @Path("/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listMacros() {
        JSONArray macros = new JSONArray();
        try {
            for (String key : jobMacroManager.getKeys()) {
                JobMacro macro = jobMacroManager.getEntity(key);
                macros.put(macro.toJSON().put("macro", "").put("name", key));
            }
            return Response.ok(macros.toString()).build();
        } catch (Exception ex)  {
            log.warn("", ex);
            return Response.serverError().entity(ex.toString()).build();
        }
    }

    @GET
    @Path("/map")
    @Produces(MediaType.APPLICATION_JSON)
    public Response mapMacros() {
        JSONObject macros = new JSONObject();
        try {
            for (String key : jobMacroManager.getKeys()) {
                JobMacro macro = jobMacroManager.getEntity(key);
                macros.put(key, macro.toJSON());
            }
            return Response.ok(macros.toString()).build();
        } catch (Exception ex)  {
            log.warn("", ex);
            return Response.serverError().entity(ex.toString()).build();
        }
    }

    @GET
    @Path("/get")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getMacros(@QueryParam("label") String label) throws Exception {
        JobMacro macro = jobMacroManager.getEntity(label);
        JSONObject macroJson = macro.toJSON();
        macroJson.put("modified", macro.getModified());
        macroJson.put("owner", macro.getOwner());
        macroJson.put("name", label);
        return Response.ok(macroJson.toString()).build();
    }

    @POST
    @Path("/save")
    @Produces(MediaType.APPLICATION_JSON)
    public Response saveMacro(@QueryParam("pairs") KVPairs kv, @Auth User user) {
        try {
            if (!kv.hasKey("label")) {
                throw new Exception("missing field");
            }
            String label = kv.getValue("label");
            JobMacro oldMacro = jobMacroManager.getEntity(label);
            String description = kv.getValue("description", oldMacro != null ? oldMacro.getDescription() : null);
            String owner = kv.getValue("owner", oldMacro != null ? oldMacro.getOwner() : user.getUsername());
            String macro = kv.getValue("macro", oldMacro != null ? oldMacro.getMacro() : null);
            if (macro != null && macro.length() > Spawn.inputMaxNumberOfCharacters) {
                throw new IllegalArgumentException("Macro length of " + macro.length()
                                                   + " characters is greater than max length of "
                                                   + Spawn.inputMaxNumberOfCharacters);
            }
            JobMacro jobMacro = new JobMacro(owner, description, macro);
            jobMacroManager.putEntity(label, jobMacro, true);
            return Response.ok().entity(jobMacro.toJSON().put("macro", "").put("name", label).toString()).build();
        } catch (Exception ex) {
            return Response.serverError().entity(ex.toString()).build();
        }
    }

    @POST
    @Path("/delete")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteMacro(@QueryParam("pairs") KVPairs kv, @Auth User user) {
        try {
            String name = kv.getValue("name");
            if (name == null) {
                return Response.serverError().entity("missing macro name").build();
            }
            if (jobMacroManager.deleteEntity(name)) {
                return Response.ok().build();
            } else {
                return Response.serverError().entity("macro may be used by a job").build();
            }
        } catch (Exception ex) {
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }
}
