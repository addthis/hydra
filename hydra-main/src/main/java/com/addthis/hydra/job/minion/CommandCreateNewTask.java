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

import com.addthis.hydra.job.mq.CommandTaskNew;
import com.addthis.hydra.job.mq.CoreMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommandCreateNewTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CommandCreateNewTask.class);

    private Minion minion;
    private CoreMessage core;

    public CommandCreateNewTask(Minion minion, CoreMessage core) {
        this.minion = minion;
        this.core = core;
    }

    @Override
    public void run() {
        CommandTaskNew newTask = (CommandTaskNew) core;
        JobTask task = minion.tasks.get(newTask.getJobKey().toString());
        if (task == null) {
            log.warn("[task.new] creating " + newTask.getJobKey());
            try {
                minion.createNewTask(newTask.getJobUuid(), newTask.getNodeID());
            } catch (ExecException e) {
                log.warn("Error restoring task state: " + e, e);
            }
        } else {
            // Make sure the id/node # were set correctly
            task.id = newTask.getJobUuid();
            task.node = newTask.getNodeID();
            log.warn("[task.new] skip existing " + newTask.getJobKey());
        }
    }
}
