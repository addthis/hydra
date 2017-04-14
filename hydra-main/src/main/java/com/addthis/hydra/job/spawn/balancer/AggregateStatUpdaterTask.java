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
package com.addthis.hydra.job.spawn.balancer;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.hydra.job.mq.HostState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Update aggregate cluster statistics periodically */
class AggregateStatUpdaterTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(AggregateStatUpdaterTask.class);
    private static final AtomicLong errorCounter = new AtomicLong(0);

    private final SpawnBalancer spawnBalancer;

    public AggregateStatUpdaterTask(SpawnBalancer spawnBalancer) {
        this.spawnBalancer = spawnBalancer;
    }

    @Override public void run() {
        try {
            List<HostState> allHosts = spawnBalancer.hostManager.listHostStatus(null);
            spawnBalancer.updateAggregateStatistics(allHosts);
        } catch (Exception e) {
            // Only log full stack trace every 10 errors
            if ((errorCounter.getAndIncrement() % 10) == 0) {
                log.error("Error updating aggregate host stats for SpawnBalancer: {}", e.getMessage(), e);
            } else {
                log.error("Error updating aggregate host stats for SpawnBalancer: {}", e.getMessage());
            }
        }
    }
}
