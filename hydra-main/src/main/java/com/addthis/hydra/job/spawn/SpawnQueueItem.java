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
package com.addthis.hydra.job.spawn;

import com.addthis.codec.codables.Codable;
import com.addthis.hydra.job.mq.JobKey;

/**
 * A class representing an item that can sit on Spawn's queue.
 */
public class SpawnQueueItem extends JobKey implements Codable {

    private int priority;

    private final long creationTime; // When this task was added to the queue

    // Need this empty constructor for deserialization
    public SpawnQueueItem() {
        super();
        priority = 0;
        creationTime = System.currentTimeMillis();
    }

    public SpawnQueueItem(JobKey key, int priority) {
        this.setJobUuid(key.getJobUuid());
        this.setNodeNumber(key.getNodeNumber());
        this.priority = priority;
        this.creationTime = System.currentTimeMillis();
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setIgnoreQuiesce(boolean ignoreQuiesce) // Necessary for correct deserialization
    {
        this.priority = ignoreQuiesce ? 1 : 0;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public String toString() {
        return "SpawnQueueItem{jobKey=" + this.getJobKey() + ",priority=" + priority + '}';
    }
}
