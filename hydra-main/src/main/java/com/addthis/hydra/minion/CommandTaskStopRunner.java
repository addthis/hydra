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
package com.addthis.hydra.minion;

import java.util.List;

import com.addthis.basis.util.LessFiles;

import com.addthis.hydra.job.mq.CommandTaskStop;
import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.StatusTaskEnd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommandTaskStopRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CommandTaskStopRunner.class);

    private Minion minion;
    private CoreMessage core;

    public CommandTaskStopRunner(Minion minion, CoreMessage core) {
        this.minion = minion;
        this.core = core;
    }

    @Override
    public void run() {
        CommandTaskStop stop = (CommandTaskStop) core;
        log.warn("[task.stop] request " + stop.getJobKey() + " count @ " + stop.getRunCount());
        minion.removeJobFromQueue(stop.getJobKey(), false);
        List<JobTask> match = minion.getMatchingJobs(stop);
        if (match.size() == 0 && stop.getNodeID() != null && stop.getNodeID() >= 0) {
            log.warn("[task.stop] unmatched stop for " + stop.getJobUuid() + " / " + stop.getNodeID());
            minion.sendStatusMessage(new StatusTaskEnd(minion.uuid, stop.getJobUuid(), stop.getNodeID(), 0, 0, 0));
        }
        for (JobTask task : match) {
            if (!task.getConfigDir().exists()) {
                LessFiles.initDirectory(task.getConfigDir());
            }
            if (task.isRunning() || task.isReplicating() || task.isBackingUp()) {
                if (!stop.force() && task.isBackingUp()) {
                    log.warn("[task.stop] " + task.getName() + " wasn't terminated because task was backing up and the stop wasn't a kill");
                } else if (!stop.force() && task.isReplicating() && task.getRebalanceSource() == null) {
                    log.warn("[task.stop] " + task.getName() + " wasn't terminated because task was replicating and the stop wasn't a kill");
                } else {
                    task.stopWait(stop.force());
                    log.warn("[task.stop] " + task.getName());
                }
            } else if (stop.force()) {
                log.warn("[task.stop] " + task.getName() + " force stop idle task");
                task.sendEndStatus(task.findLastJobStatus());
            }
        }
        minion.writeState(true);
    }

}
