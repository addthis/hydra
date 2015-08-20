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

import javax.annotation.Nullable;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

public enum JobTaskState {
    IDLE(0), BUSY(1), ERROR(2), ALLOCATED(3), BACKUP(4), REPLICATE(5),
    UNKNOWN(6), REBALANCE(7), REVERT(8), QUEUED_HOST_UNAVAIL(9), SWAPPING(10),
    QUEUED(11), MIGRATING(12), FULL_REPLICATE(13), QUEUED_NO_SLOT(14);

    private final int value;

    private static final Map<Integer, JobTaskState> map = new HashMap<>();

    static {
        for (JobTaskState state : JobTaskState.values()) {
            map.put(state.getValue(), state);
        }
    }

    private static final Set<JobTaskState> inactiveStates = ImmutableSet.of(IDLE, ERROR, UNKNOWN, QUEUED_HOST_UNAVAIL, QUEUED, QUEUED_NO_SLOT);
    private static final Set<JobTaskState> queuedStates   = ImmutableSet.of(QUEUED_HOST_UNAVAIL, QUEUED, QUEUED_NO_SLOT);
    private static final Map<JobTaskState, Set<JobTaskState>> transitions;

    static {
        transitions = new HashMap<>();
        transitions.put(IDLE, EnumSet.of(ALLOCATED, BACKUP, REPLICATE, REBALANCE, REVERT, BUSY, QUEUED_HOST_UNAVAIL, SWAPPING, QUEUED, FULL_REPLICATE, QUEUED_NO_SLOT));
        transitions.put(ALLOCATED, EnumSet.of(IDLE, BUSY, ERROR, FULL_REPLICATE, REPLICATE, BACKUP, REBALANCE));
        transitions.put(BUSY, EnumSet.of(IDLE, REPLICATE, BACKUP, ERROR));
        transitions.put(REPLICATE, EnumSet.of(IDLE, BACKUP, ERROR, REBALANCE));
        transitions.put(BACKUP, EnumSet.of(IDLE, REPLICATE, ERROR));
        transitions.put(ERROR, EnumSet.of(IDLE, REVERT));
        transitions.put(UNKNOWN, EnumSet.of(IDLE));
        transitions.put(REBALANCE, EnumSet.of(IDLE, FULL_REPLICATE, REPLICATE, QUEUED, ERROR));
        transitions.put(REVERT, EnumSet.of(IDLE, FULL_REPLICATE, REPLICATE));
        transitions.put(QUEUED_HOST_UNAVAIL, EnumSet.of(IDLE, QUEUED));
        transitions.put(QUEUED_NO_SLOT, EnumSet.of(IDLE, QUEUED, QUEUED_HOST_UNAVAIL));
        transitions.put(SWAPPING, EnumSet.of(IDLE, ERROR));
        transitions.put(QUEUED, EnumSet.of(IDLE, QUEUED_HOST_UNAVAIL, ALLOCATED, SWAPPING, ERROR, FULL_REPLICATE, QUEUED_NO_SLOT));
        transitions.put(MIGRATING, EnumSet.of(IDLE, FULL_REPLICATE, REPLICATE, QUEUED, ERROR));
        transitions.put(FULL_REPLICATE, EnumSet.of(IDLE, BACKUP, ERROR, REBALANCE));
    }

    private JobTaskState(int value) {
        this.value = value;
    }

    public boolean isActiveState() {
        return !inactiveStates.contains(this);
    }

    public boolean isQueuedState() {
        return queuedStates.contains(this);
    }

    public boolean canTransition(JobTaskState state) {
        return this == state || transitions.get(this).contains(state);
    }

    public int getValue() {
        return value;
    }

    @Nullable
    public static JobTaskState makeState(int value) {
        return map.get(value);
    }
}
