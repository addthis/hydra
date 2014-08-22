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
package com.addthis.hydra.job.minion;

import java.io.File;

import com.addthis.hydra.job.mq.CommandTaskDelete;
import com.addthis.hydra.job.mq.CoreMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommandTaskDeleteRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CommandTaskDeleteRunner.class);

    private Minion minion;
    CoreMessage core;

    public CommandTaskDeleteRunner(Minion minion, CoreMessage core) {
        this.minion = minion;
        this.core = core;
    }

    @Override
    public void run() {
        CommandTaskDelete delete = (CommandTaskDelete) core;
        log.warn("[task.delete] " + delete.getJobKey());
        minion.minionStateLock.lock();
        try {
            for (JobTask task : minion.getMatchingJobs(delete)) {
                minion.stopped.put(delete.getJobUuid(), delete.getRunCount());
                boolean terminated = task.isRunning() && task.stopWait(true);
                task.setDeleted(true);
                minion.tasks.remove(task.getJobKey().toString());
                log.warn("[task.delete] " + task.getJobKey() + " terminated=" + terminated);
                minion.writeState();
            }
            File taskDirFile = new File(minion.rootDir + "/" + delete.getJobUuid() + (delete.getNodeID() != null ? "/" + delete.getNodeID() : ""));
            if (taskDirFile.exists() && taskDirFile.isDirectory()) {
                minion.minionTaskDeleter.submitPathToDelete(taskDirFile.getAbsolutePath());
            }
        } finally {
            minion.minionStateLock.unlock();
        }

    }
}
