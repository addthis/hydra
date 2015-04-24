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

import java.util.ArrayList;
import java.util.List;

import com.addthis.hydra.job.mq.CommandTaskUpdateReplicas;
import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.ReplicaTarget;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommandTaskUpdateReplicasRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CommandTaskUpdateReplicasRunner.class);

    private Minion minion;
    private CoreMessage core;

    public CommandTaskUpdateReplicasRunner(Minion minion, CoreMessage core) {
        this.minion = minion;
        this.core = core;
    }

    @Override
    public void run() {
        CommandTaskUpdateReplicas updateReplicas = (CommandTaskUpdateReplicas) core;
        JobTask task = minion.tasks.get(updateReplicas.getJobKey().toString());
        if (task != null) {
            synchronized (task) {
                List<ReplicaTarget> newReplicas = new ArrayList<>();
                if (task.replicas != null) {
                    for (ReplicaTarget replica : task.replicas) {
                        if (!updateReplicas.getFailedHosts().contains(replica.getHostUuid())) {
                            newReplicas.add(replica);
                        }
                    }
                }
                task.setReplicas(newReplicas.toArray(new ReplicaTarget[newReplicas.size()]));
                List<ReplicaTarget> failureRecoveryReplicas = updateReplicas.getNewReplicaHosts();
                task.setFailureRecoveryReplicas(failureRecoveryReplicas.toArray(new ReplicaTarget[failureRecoveryReplicas.size()]));
            }
        }
    }
}
