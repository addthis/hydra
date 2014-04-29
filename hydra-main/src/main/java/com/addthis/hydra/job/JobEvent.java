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

import java.util.HashMap;
import java.util.Map;

public enum JobEvent {
    STOP(0), KILL(1), START(2), DELETE(3), REVERT(4), FINISH(5), SCHEDULED(6), ERROR(7);

    private final int value;

    private static final Map<Integer, JobEvent> map = new HashMap<>();

    static {
        for (JobEvent state : JobEvent.values()) {
            map.put(state.getValue(), state);
        }
    }

    private JobEvent(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
    
}
