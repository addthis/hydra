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
package com.addthis.hydra.job.spawn;

import java.util.Collection;

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobParameter;
import com.addthis.hydra.job.JobTask;
import com.addthis.hydra.job.JobTaskErrorCode;
import com.addthis.hydra.job.mq.CommandTaskKick;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledTaskKick implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskKick.class);

    private Spawn spawn;
    public String jobId;
    public Collection<JobParameter> jobParameters;
    public String jobConfig;
    public String rawJobConfig;
    public SpawnMQ spawnMQ;
    public CommandTaskKick kick;
    public Job job;
    public JobTask task;

    public ScheduledTaskKick(Spawn spawn,
                             String jobId,
                             Collection<JobParameter> jobParameters,
                             String jobConfig,
                             String rawJobConfig,
                             SpawnMQ spawnMQ,
                             CommandTaskKick kick,
                             Job job,
                             JobTask task) {
        this.spawn = spawn;
        this.jobId = jobId;
        this.jobParameters = jobParameters;
        this.jobConfig = jobConfig;
        this.rawJobConfig = rawJobConfig;
        this.spawnMQ = spawnMQ;
        this.kick = kick;
        this.job = job;
        this.task = task;
    }

    @Override public void run() {
        try {
            if (jobConfig == null) {
                jobConfig = spawn.expandJob(jobId, jobParameters, rawJobConfig);
            }
            kick.setConfig(jobConfig);
            spawnMQ.sendJobMessage(kick);
            if (spawn.getSystemManager().debug("-task-")) {
                log.info("[task.schedule] assigned {}[{}/{}] to {}", jobId, kick.getNodeID(), kick.getJobNodes() - 1,
                         kick.getHostUuid());
            }
        } catch (Exception e) {
            log.warn("failed to kick job {} task {} on host {}", jobId, kick.getNodeID(), kick.getHostUuid(), e);
            spawn.jobLock.lock();
            try {
                job.errorTask(task, JobTaskErrorCode.KICK_ERROR);
            } finally {
                spawn.jobLock.unlock();
            }
        }
    }
}
