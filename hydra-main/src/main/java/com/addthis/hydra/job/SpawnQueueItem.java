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

import com.addthis.codec.Codec;
import com.addthis.hydra.job.mq.JobKey;

/**
 * A class representing an item that can sit on Spawn's queue.
 */
public class SpawnQueueItem extends JobKey implements Codec.Codable {

    @Codec.Set(codable = true)
    private boolean ignoreQuiesce; // Whether this task is allowed to kick even if Spawn is quiesced

    private final long creationTime; // When this task was added to the queue

    // Need this empty constructor for deserialization
    public SpawnQueueItem() {
        super();
        ignoreQuiesce = false;
        creationTime = System.currentTimeMillis();
    }

    public SpawnQueueItem(JobKey key, boolean ignoreQuiesce) {
        this.setJobUuid(key.getJobUuid());
        this.setNodeNumber(key.getNodeNumber());
        this.ignoreQuiesce = ignoreQuiesce;
        this.creationTime = System.currentTimeMillis();
    }

    public long getCreationTime() {
        return creationTime;
    }

    public boolean getIgnoreQuiesce() {
        return ignoreQuiesce;
    }

    public void setIgnoreQuiesce(boolean ignoreQuiesce) // Necessary for correct deserialization
    {
        this.ignoreQuiesce = ignoreQuiesce;
    }

    @Override
    public String toString() {
        return "SpawnQueueItem{jobKey=" + this.getJobKey() + ",canIgnoreQuiesce=" + ignoreQuiesce + '}';
    }
}
