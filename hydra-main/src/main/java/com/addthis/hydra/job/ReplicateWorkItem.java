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
package com.addthis.hydra.job;

import java.io.File;

import com.addthis.hydra.job.minion.*;
import com.addthis.hydra.job.mq.ReplicaTarget;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ReplicateWorkItem extends MinionWorkItem {

    private static Logger log = LoggerFactory.getLogger(ReplicateWorkItem.class);
    private String rebalanceSource;
    private String rebalanceTarget;

    public ReplicateWorkItem(File jobDir, File pidFile, File runFile, File doneFile, com.addthis.hydra.job.minion.JobTask task, String rebalanceSource, String rebalanceTarget, boolean execute) {
        super(jobDir, pidFile, runFile, doneFile, task, execute);
        this.rebalanceSource = rebalanceSource;
        this.rebalanceTarget = rebalanceTarget;
    }

    @Override
    public String getLogPrefix() {
        return "[replicate.work.item]";
    }

    @Override
    public long getStartTime() {
        return task.getReplicateStartTime();
    }

    @Override
    public void setStartTime(long start) {
        task.setReplicateStartTime(start);
    }

    @Override
    public void sendFinishStatusMessages(int exit) throws Exception {
        if (exit == 0) {
            task.clearFailureReplicas();
            informReplicaHosts();
            task.execBackup(rebalanceSource, rebalanceTarget, true);
        } else if (task.getRebalanceSource() != null) {
            task.sendEndStatus(JobTaskErrorCode.REBALANCE_PAUSE);
        }
        else {
            if (!doneFile.exists()) {
                doneFile.createNewFile();
            }
            task.sendEndStatus(JobTaskErrorCode.EXIT_REPLICATE_FAILURE);
        }
    }

    private void informReplicaHosts() {
        if (task.getReplicas() != null) {
            for (ReplicaTarget replica : task.getReplicas()) {
                log.warn("[replicate] sending command.task.new to " + replica.getHostUuid() + " / " + task.getName());
                task.sendNewStatusToReplicaHost(replica.getHostUuid());
            }
        }
    }

    @Override
    public void executeWaitingCommands() {
    }

    @Override
    public void clear() {
        log.warn(getLogPrefix() + " clearing " + task.getName());
        setStartTime(0);
        task.setProcess(null);
        task.save();
    }

    @Override
    public int getExitStatusFromString(String exitString) {
        if (exitString == null) {
            return 0;
        }
        try {
            return Integer.parseInt(exitString);
        } catch (NumberFormatException ex) {
            log.warn("Unparsable exit code: " + exitString);
            return 0;
        }
    }
}
