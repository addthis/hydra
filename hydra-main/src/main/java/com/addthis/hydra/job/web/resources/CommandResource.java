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
import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessStrings;

import com.addthis.hydra.job.entity.JobCommand;
import com.addthis.hydra.job.entity.JobEntityManager;
import com.addthis.maljson.JSONArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/command")
public class CommandResource {

    private static final Logger log = LoggerFactory.getLogger(CommandResource.class);

    private final JobEntityManager<JobCommand> jobCommandManager;

    public CommandResource(JobEntityManager<JobCommand> jobCommandManager) {
        this.jobCommandManager = jobCommandManager;
    }

    @GET
    @Path("/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listCommands() {
        JSONArray commands = new JSONArray();
        try {
            for (String key : jobCommandManager.getKeys()) {
                JobCommand command = jobCommandManager.getEntity(key);
                commands.put(command.toJSON().put("name", key));
            }
            return Response.ok(commands.toString()).build();
        } catch (Exception ex)  {
            log.warn("", ex);
            return Response.serverError().entity(ex.toString()).build();
        }
    }

    @GET
    @Path("/get")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCommands(@QueryParam("command") String key) throws Exception {
        return Response.ok(jobCommandManager.getEntity(key).toJSON().put("name", key).toString()).build();
    }

    @POST
    @Path("/save")
    @Produces(MediaType.APPLICATION_JSON)
    public Response postCommando(@QueryParam("pairs") KVPairs kv,
                                 @QueryParam("user")  String user,
                                 @QueryParam("token") String token) {
        try {
            String label = kv.getValue("name");
            String command = kv.getValue("command", "").trim();
            String owner = kv.getValue("owner", "unknown").trim();
            if ((label == null)) {
                throw new Exception("missing required field 'name'");
            }
            if (command.isEmpty()) {
                throw new Exception("missing required field 'command'");
            }
            String[] cmdtok = LessStrings.splitArray(command, ",");
            for (int i = 0; i < cmdtok.length; i++) {
                cmdtok[i] = LessBytes.urldecode(cmdtok[i]);
            }
            JobCommand jobCommand = new JobCommand(owner, cmdtok, kv.getIntValue("reqCPU", 0), kv.getIntValue("reqMEM", 0), kv.getIntValue("reqIO", 0));
            jobCommandManager.putEntity(label, jobCommand, true);
            return Response.ok().entity(jobCommand.toJSON().put("name", label).toString()).build();
        } catch (Exception ex) {
            log.info("Error while saving command: ", ex);
            return Response.serverError().entity(ex.toString()).build();
        }
    }

    @POST
    @Path("/delete")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteCommand(@QueryParam("pairs") KVPairs kv,
                                  @QueryParam("user")  String user,
                                  @QueryParam("token") String token) {
        try {
            String name = kv.getValue("name");
            if (name == null) {
                return Response.serverError().entity("missing command name").build();
            }
            if (jobCommandManager.deleteEntity(name)) {
                return Response.ok().build();
            } else {
                return Response.serverError().entity("command may be used by a job").build();
            }
        } catch (Exception ex) {
            log.info("Error while deleting command: ", ex);
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }
}
