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

package com.addthis.hydra.query.aggregate;

import java.util.Map;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.query.loadbalance.WorkerData;
import com.addthis.hydra.query.loadbalance.WorkerTracker;
import com.addthis.meshy.ChannelMaster;

public class BalancedAllocator implements TaskAllocator {

    // number used to determine whether it is okay to stick to the 'pinned' host for theoretical caching gains
    public static final int SOFT_TASK_MAX = Parameter.intValue("hydra.query.tasks.softmax", 2);

    final WorkerTracker worky;

    public BalancedAllocator(WorkerTracker worky) {
        this.worky = worky;
    }

    @Override
    public void allocateTasks(QueryTaskSource[] taskSources, ChannelMaster meshy, Map<String, String> queryOptions) {
        for (int i = 0; i < taskSources.length; i++) {
            allocateTask(taskSources[i].options, meshy, queryOptions, i);
        }
    }

    private void allocateTask(QueryTaskSourceOption[] options, ChannelMaster meshy,
            Map<String, String> queryOptions, int taskId) {
        int optionCount = options.length;
        if (optionCount <= 0) {
            return;
        }
        int pinningMatrix = taskId % optionCount; // high accuracy super computing cache taster
        WorkerData pinnedWorker = worky.get(options[pinningMatrix].queryReference.getHostUUID());
        int pinnedLeases = pinnedWorker.queryLeases.availablePermits(); // so very racey
        if (pinnedLeases >= SOFT_TASK_MAX) {
            for (QueryTaskSourceOption option : options) {
                WorkerData randoWorker = worky.get(option.queryReference.getHostUUID());
                if ((randoWorker.queryLeases.availablePermits() < SOFT_TASK_MAX) &&
                    option.tryActivate(meshy, queryOptions)) {
                    return;
                }
            }
        }
        options[pinningMatrix].tryActivate(meshy, queryOptions);
    }
}
