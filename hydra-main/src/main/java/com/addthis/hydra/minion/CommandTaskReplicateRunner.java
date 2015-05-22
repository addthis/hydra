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

import java.io.File;

import com.addthis.hydra.job.mq.CommandTaskReplicate;
import com.addthis.hydra.job.mq.CoreMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommandTaskReplicateRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CommandTaskReplicateRunner.class);

    private Minion minion;
    private CoreMessage core;

    public CommandTaskReplicateRunner(Minion minion, CoreMessage core) {
        this.minion = minion;
        this.core = core;
    }

    @Override
    public void run() {
        CommandTaskReplicate replicate = (CommandTaskReplicate) core;
        JobTask task = minion.tasks.get(replicate.getJobKey().toString());
        if (task != null) {
            if (task.jobDir == null) {
                task.jobDir = task.getLiveDir();
            }
            if (!task.jobDir.exists()) {
                log.warn("[task.replicate] aborted because there is no directory for " + task.getJobKey() + " yet: " + task.jobDir);
            } else if (!task.isRunning() && !task.isReplicating() && !task.isBackingUp()) {
                log.warn("[task.replicate] starting " + replicate.getJobKey());
                minion.removeJobFromQueue(replicate.getJobKey(), false);
                if (!task.isComplete()) {
                    // Attempt to revert to the latest complete backup, if one can be found
                    String latestCompleteBackup = task.getBackupByRevision(0, "gold");
                    if (latestCompleteBackup != null) {
                        task.promoteBackupToLive(new File(task.getJobDir(), latestCompleteBackup), task.getLiveDir());
                    }
                }
                try {
                    task.setReplicas(replicate.getReplicas());
                    task.execReplicate(replicate.getRebalanceSource(), replicate.getRebalanceTarget(), true, true, replicate.wasQueued());
                } catch (Exception e) {
                    log.warn("[task.replicate] received exception after replicate request for " + task.getJobKey() + ": " + e, e);
                }
            } else {
                log.warn("[task.replicate] skip running " + replicate.getJobKey());
            }
        }
    }
}
