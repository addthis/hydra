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

import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * re-kicks jobs which are on a repeating schedule
 */
class JobRekickTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(JobRekickTask.class);

    private Spawn spawn;

    public JobRekickTask(Spawn spawn) {this.spawn = spawn;}

    @Override public void run() {
        boolean kicked;

        do {
            kicked = false;
            /*
             * cycle through jobs and look for those that need nodes
             * allocated. lock to prevent other RPCs from conflicting with scheduling.
             */
            try {
                if (!spawn.getSystemManager().isQuiesced()) {
                    String[] jobids = null;
                    // Not needed
                    spawn.acquireJobLock();
                    try {
                        jobids = new String[spawn.spawnState.jobs.size()];
                        jobids = spawn.spawnState.jobs.keySet().toArray(jobids);
                    } finally {
                        spawn.releaseJobLock();
                    }
                    long clock = System.currentTimeMillis();
                    for (String jobid : jobids) {
                        Job job = spawn.getJob(jobid);
                        if (job == null) {
                            log.warn("ERROR: missing job for id " + jobid);
                            continue;
                        }
                        if (job.getState() == JobState.IDLE && job.getStartTime() == null && job.getEndTime() == null) {
                            job.setEndTime(clock);
                        }
                        // check for recurring jobs (that aren't already running)
                        if (job.shouldAutoRekick(clock)) {
                            try {
                                if (spawn.scheduleJob(job, 0)) {
                                    log.info("[schedule] rekicked " + job.getId());
                                    kicked = true;
                                }
                            } catch (Exception ex) {
                                log.warn("[schedule] ex while rekicking, disabling " + job.getId());
                                job.setEnabled(false);
                                spawn.updateJob(job);
                                throw new Exception(ex);
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                log.warn("auto rekick failed: ", ex);
            }
        }
        while (kicked);
    }
}
