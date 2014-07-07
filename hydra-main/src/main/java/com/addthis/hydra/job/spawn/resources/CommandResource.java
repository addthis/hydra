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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Strings;

import com.addthis.hydra.job.JobCommand;
import com.addthis.hydra.job.Spawn;
import com.addthis.hydra.job.spawn.jersey.User;
import com.addthis.maljson.JSONArray;

import com.yammer.dropwizard.auth.Auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@Path("/command")
public class CommandResource {

    private static Logger log = LoggerFactory.getLogger(CommandResource.class);

    private final Spawn spawn;

    private static final String defaultUser = "UNKNOWN_USER";

    public CommandResource(Spawn spawn) {
        this.spawn = spawn;
    }

    @GET
    @Path("/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listCommands() {
        JSONArray commands = new JSONArray();
        try {
            for (String key : spawn.listCommands()) {
                JobCommand command = spawn.getCommand(key);
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
        return Response.ok(spawn.getCommand(key).toJSON().put("name", key).toString()).build();
    }

    @POST
    @Path("/save")
    @Produces(MediaType.APPLICATION_JSON)
    public Response postCommando(@QueryParam("pairs") KVPairs kv, @Auth User user) {
        try {
            String label = kv.getValue("name");
            String command = kv.getValue("command", "").trim();
            String owner = kv.getValue("owner", "unknown").trim();
            if (label == null || command.length() == 0) {
                throw new Exception("missing required field");
            }
            String cmdtok[] = Strings.splitArray(command, ",");
            for (int i = 0; i < cmdtok.length; i++) {
                cmdtok[i] = Bytes.urldecode(cmdtok[i]);
            }
            JobCommand jobCommand = new JobCommand(owner, cmdtok, kv.getIntValue("reqCPU", 0), kv.getIntValue("reqMEM", 0), kv.getIntValue("reqIO", 0));
            spawn.putCommand(label, jobCommand, true);
            return Response.ok().entity(jobCommand.toJSON().put("name", label).toString()).build();
        } catch (Exception ex) {
            return Response.serverError().entity(ex.toString()).build();
        }
    }

    @POST
    @Path("/delete")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteCommand(@QueryParam("pairs") KVPairs kv, @Auth User user) {
        try {
            String name = kv.getValue("name");
            if (name == null) {
                return Response.serverError().entity("missing command name").build();
            }
            spawn.deleteCommand(name);
            return Response.ok().build();
        } catch (Exception ex) {
            return Response.serverError().entity("command delete failed").build();
        }
    }
}
