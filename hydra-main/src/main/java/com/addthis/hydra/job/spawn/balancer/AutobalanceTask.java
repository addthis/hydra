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

import com.addthis.hydra.job.JobTaskMoveAssignment;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import org.joda.time.DateTimeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class performs job/host rebalancing at specified intervals
 */
class AutobalanceTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(AutobalanceTask.class);

    private final SpawnBalancer spawnBalancer;
    private final MinuteSinceLastBalanceGauge minutesSinceLastBalanceGauge;

    private long lastJobAutobalanceTime = 0L;
    private long lastHostAutobalanceTime = 0L;
    private boolean rebalanceWeightToggle = false;

    public AutobalanceTask(SpawnBalancer spawnBalancer) {
        this.spawnBalancer = spawnBalancer;
        this.minutesSinceLastBalanceGauge = new MinuteSinceLastBalanceGauge();
        Metrics.newGauge(AutobalanceTask.class, "minutesSinceLastBalance", minutesSinceLastBalanceGauge);
    }

    @Override
    public void run() {
        long now = System.currentTimeMillis();
        // Don't autobalance if spawn is quiesced, busy, etc.
        if (spawnBalancer.okayToAutobalance()) {
            if ((now - lastHostAutobalanceTime) > spawnBalancer.getConfig().getHostAutobalanceIntervalMillis()) {
                log.warn("Performing host autobalance.");
                RebalanceWeight weight = rebalanceWeightToggle ? RebalanceWeight.HEAVY : RebalanceWeight.LIGHT;
                rebalance(RebalanceType.HOST, weight);
                rebalanceWeightToggle = !rebalanceWeightToggle;
                lastHostAutobalanceTime = now;
                minutesSinceLastBalanceGauge.setLastBalanceTime(now);
            } else if ((now - lastJobAutobalanceTime) > spawnBalancer.getConfig().getJobAutobalanceIntervalMillis()) {
                log.warn("Performing job autobalance.");
                rebalance(RebalanceType.JOB, RebalanceWeight.HEAVY);
                lastJobAutobalanceTime = now;
                minutesSinceLastBalanceGauge.setLastBalanceTime(now);
            }
        }
    }

    private void rebalance(RebalanceType rbType, RebalanceWeight rbWeight) {
        List<JobTaskMoveAssignment> assignments = spawnBalancer.getAssignmentsForAutoBalance(rbType, rbWeight);
        spawnBalancer.spawn.executeReallocationAssignments(assignments, false);
    }

    /** Conveninece Guage implementation for tracking how long since last autobalance */
    private static class MinuteSinceLastBalanceGauge extends Gauge<Long> {

        private volatile long lastBalanceTime = System.currentTimeMillis();

        private void setLastBalanceTime(long millis) {
            lastBalanceTime = millis;
        }

        @Override public Long value() {
            return (System.currentTimeMillis() - lastBalanceTime) / DateTimeConstants.MILLIS_PER_MINUTE;
        }
    }
}
