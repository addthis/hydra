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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;

import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Optional;

@Path("/task")
public class TaskResource {

    private final Spawn spawn;

    public TaskResource(Spawn spawn) {
        this.spawn = spawn;
    }

    @GET
    @Path("/start")
    @Produces(MediaType.APPLICATION_JSON)
    public Response startTask(@QueryParam("job") Optional<String> jobId,
            @QueryParam("task") Optional<Integer> task) {
        try {
            spawn.startTask(jobId.or(""), task.or(-1), true, 1, false);
            return Response.ok().build();
        } catch (Exception ex) {
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }

    @GET
    @Path("/move")
    @Produces(MediaType.APPLICATION_JSON)
    public Response moveTask(@QueryParam("job") String jobId,
                             @QueryParam("task") Integer task,
                             @QueryParam("source") String source,
                             @QueryParam("target") String target) {
        if (jobId == null) {
            return Response.serverError().entity("Missing required parameter 'job'").build();
        } else if (task == null) {
            return Response.serverError().entity("Missing required parameter 'task'").build();
        } else if (source == null) {
            return Response.serverError().entity("Missing required parameter 'source'").build();
        } else if (target == null) {
            return Response.serverError().entity("Missing required parameter 'target'").build();
        } else if (source.equals(target)) {
            return Response.serverError().entity("source and target cannot be identical").build();
        }
        JobTask jobTask = spawn.getTask(jobId, task);
        if (jobTask == null) {
            return Response.serverError().entity("Failure to find job " + jobId + " and task " + target).build();
        } else {
            boolean success = spawn.moveTask(jobTask.getJobKey(), source, target);
            return Response.ok(Boolean.toString(success)).build();
        }
    }

    @GET
    @Path("/stop")
    @Produces(MediaType.APPLICATION_JSON)
    public Response stopTask(@QueryParam("job") Optional<String> jobId,
            @QueryParam("task") Optional<Integer> task) {
        try {
            spawn.stopTask(jobId.or(""), task.or(-1));
            return Response.ok().build();
        } catch (Exception ex) {
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }

    @GET
    @Path("/kill")
    @Produces(MediaType.APPLICATION_JSON)
    public Response killTask(@QueryParam("job") Optional<String> jobId,
            @QueryParam("task") Optional<Integer> task) {
        try {
            spawn.killTask(jobId.or(""), task.or(-1));
            return Response.ok().build();
        } catch (Exception ex) {
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }

    @GET
    @Path("/get")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTask(@QueryParam("job") Optional<String> jobId,
            @QueryParam("task") Optional<Integer> taskId) {
        try {
            Job job = spawn.getJob(jobId.get());
            List<JobTask> tasks = job.getCopyOfTasks();
            for (JobTask task : tasks) {
                if (task.getTaskID() == taskId.get()) {
                    JSONObject json = CodecJSON.encodeJSON(task);
                    return Response.ok(json.toString()).build();
                }
            }
            return Response.status(Response.Status.NOT_FOUND).entity("Task with id " + taskId.get() + " was not found for job " + jobId.get()).build();
        } catch (Exception ex) {
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }
}
