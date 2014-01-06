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

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.addthis.hydra.job.Spawn;
import com.addthis.hydra.job.spawn.JobAlert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/alert")
public class AlertResource {

    private static Logger log = LoggerFactory.getLogger(AlertResource.class);

    private final Spawn spawn;

    public AlertResource(Spawn spawn) {
        this.spawn = spawn;
    }

    @GET
    @Path("/save")
    @Produces(MediaType.APPLICATION_JSON)
    public Response putAlert(
            @QueryParam("alertId") String alertId,
            @QueryParam("type") int type,
            @QueryParam("timeout") @DefaultValue("0") int timeout,
            @QueryParam("email") String email,
            @QueryParam("jobIds") String jobIds) {
        if (alertId != null && jobIds != null) {
            JobAlert jobAlert = new JobAlert(alertId, type, timeout, email, jobIds.split(","));
            spawn.putAlert(alertId, jobAlert);
            return Response.ok().build();
        }
        else {
            log.warn("Received save alert request without id; returning error");
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/delete")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteAlert(@QueryParam("alertId") String alertId) {
        spawn.removeAlert(alertId);
        return Response.ok().build();
    }

    @GET
    @Path("/fetch")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAlertState() {
         return Response.ok(spawn.fetchAlertState().toString()).build();
    }

}
