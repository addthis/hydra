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

public enum JobState {
    IDLE(0), SCHEDULED(1), RUNNING(2), DEGRADED(3), UNKNOWN(4), ERROR(5), REBALANCE(6);

    private final int value;

    private static final Map<Integer, JobState> map = new HashMap<>();

    static {
        for (JobState state : JobState.values()) {
            map.put(state.getValue(), state);
        }
    }

    private static final Map<JobState, Set<JobState>> transitions = new HashMap<>();

    static {
        transitions.put(IDLE, EnumSet.of(DEGRADED, SCHEDULED, RUNNING, ERROR, REBALANCE));
        transitions.put(SCHEDULED, EnumSet.of(IDLE, RUNNING, ERROR, REBALANCE));
        transitions.put(RUNNING, EnumSet.of(IDLE, ERROR, DEGRADED, SCHEDULED, REBALANCE));
        transitions.put(DEGRADED, EnumSet.of(IDLE));
        transitions.put(ERROR, EnumSet.of(IDLE));
        transitions.put(UNKNOWN, EnumSet.of(IDLE));
        transitions.put(REBALANCE, EnumSet.of(IDLE, SCHEDULED, ERROR));
    }

    private JobState(int value) {
        this.value = value;
    }


    public boolean canTransition(JobState state) {
        return this == state || transitions.get(this).contains(state);
    }

    public int getValue() {
        return value;
    }

    @Nullable
    public static JobState makeState(int value) {
        return map.get(value);
    }
}
