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

import java.util.UUID;

import com.addthis.basis.kv.KVPairs;

import com.addthis.hydra.job.alert.JobAlert;
import com.addthis.hydra.job.alert.JobAlertManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/alert")
public class AlertResource {

    private static final Logger log = LoggerFactory.getLogger(AlertResource.class);

    private final JobAlertManager spawn;

    public AlertResource(JobAlertManager spawn) {
        this.spawn = spawn;
    }

    @POST
    @Path("/save")
    @Produces(MediaType.APPLICATION_JSON)
    public Response putAlert(@QueryParam("pairs") KVPairs kv) {

        String alertId = kv.getValue("alertId", UUID.randomUUID().toString());
        String jobIds = kv.getValue("jobIds");
        int type = kv.getIntValue("type", -1);
        int timeout = kv.getIntValue("timeout", 0);
        String email = kv.getValue("email", "");
        String description = kv.getValue("description", "");
        String canaryPath = kv.getValue("canaryPath");
        String canaryOps = kv.getValue("canaryOps");
        String canaryRops = kv.getValue("canaryRops");
        String canaryFilter = kv.getValue("canaryFilter");
        int canaryConfigThreshold = kv.getIntValue("canaryConfigThreshold", 0);
        if (jobIds != null) {
            JobAlert jobAlert = new JobAlert(alertId, type, timeout, email, description, jobIds.split(","));
            jobAlert.setCanaryPath(canaryPath == null ? "" : canaryPath);
            jobAlert.setCanaryOps(canaryOps == null ? "" : canaryOps);
            jobAlert.setCanaryRops(canaryRops == null ? "" : canaryRops);
            jobAlert.setCanaryFilter(canaryFilter == null ? "" : canaryFilter);
            jobAlert.setCanaryConfigThreshold(canaryConfigThreshold);
            String msg = jobAlert.isValid();
            if (msg != null) {
                return Response.ok("{\"message\" : \"" + msg + "\"}").build();
            }
            spawn.putAlert(alertId, jobAlert);
            return Response.ok("{\"alertId\":\"" + alertId +"\"}").build();
        }
        else {
            return Response.ok("{\"message\" : \"Job id is missing\"}").build();
        }
    }

    @POST
    @Path("/delete")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteAlert(@QueryParam("pairs") KVPairs kv) {
        if (kv.hasKey("alertId")) {
            spawn.removeAlert(kv.getValue("alertId"));
            return Response.ok().build();
        }
        return Response.serverError().build();

    }

    @GET
    @Path("/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAlertState() {
         return Response.ok(spawn.fetchAllAlertsArray().toString()).build();
    }

    @GET
    @Path("/get")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAlert(@QueryParam("alertId") String alertId) {
        try {
            return Response.ok(spawn.getAlert(alertId)).build();
        }
        catch (Exception ex) {
            log.error("Failed to send alert config for " + alertId , ex);
            return Response.serverError().build();
        }

    }

}
