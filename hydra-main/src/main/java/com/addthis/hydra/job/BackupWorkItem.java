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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class BackupWorkItem extends MinionWorkItem {

    private static Logger log = LoggerFactory.getLogger(BackupWorkItem.class);
    private final String rebalanceSource;
    private final String rebalanceTarget;

    public BackupWorkItem(File jobDir, File pidFile, File runFile, File doneFile, com.addthis.hydra.job.minion.JobTask task, String rebalanceSource, String rebalanceTarget, boolean execute) {
        super(jobDir, pidFile, runFile, doneFile, task, execute);
        this.rebalanceSource = rebalanceSource;
        this.rebalanceTarget = rebalanceTarget;
    }

    @Override
    public void updateStats() {
        task.updateFileStats();
    }

    @Override
    public String getLogPrefix() {
        return "[backup.work.item]";
    }

    @Override
    public long getStartTime() {
        return task.getBackupStartTime();
    }

    @Override
    public void setStartTime(long start) {
        task.setBackupStartTime(start);
    }

    @Override
    public void sendFinishStatusMessages(int exit) throws Exception {
        int code = exit == 0 ? exit : JobTaskErrorCode.EXIT_BACKUP_FAILURE;
        if (!doneFile.exists()) {
            doneFile.createNewFile();
        }
        task.sendEndStatus(code, rebalanceSource, rebalanceTarget);
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
}
