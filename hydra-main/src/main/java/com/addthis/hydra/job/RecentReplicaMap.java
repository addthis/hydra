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

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

public class RecentReplicaMap {

    private final HashMap<String, Integer> replicaMap = new HashMap<>();

    private final int ALLOWED_REPLICAS_ABOVE_AVERAGE = Parameter.intValue("spawnbalance.recentreplica.replicas.aa", 3);
    private final long RECENT_REPLICA_RESET_MILLIS = Parameter.longValue("spawnbalance.recentreplica.reset", 5 * 60 * 1000);

    private long lastResetTime = 0;

    public boolean hasTooManyRecentReplicas(String hostUUID, int numHosts, int maxPerHost) {
        synchronized (replicaMap) {
            long now = JitterClock.globalTime();
            if (now > lastResetTime + RECENT_REPLICA_RESET_MILLIS) {
                lastResetTime = now;
                replicaMap.clear();
                return false;
            }
            int avg = findAverageNumberReplicas(numHosts);
            if (replicaMap.containsKey(hostUUID)) {
                if (maxPerHost > 0 && replicaMap.get(hostUUID) >= maxPerHost) {
                    return true;
                } else {
                    return replicaMap.get(hostUUID) > ALLOWED_REPLICAS_ABOVE_AVERAGE + avg;
                }
            }
            return false;
        }

    }

    public void markRecentReplica(String hostUUID) {
        synchronized (replicaMap) {
            if (!replicaMap.containsKey(hostUUID)) {
                replicaMap.put(hostUUID, 1);
            } else {
                replicaMap.put(hostUUID, replicaMap.get(hostUUID) + 1);
            }
        }
    }

    private int findAverageNumberReplicas(int numHosts) {
        synchronized (replicaMap) {
            int sum = 0;
            int numValues = Math.max(numHosts - 1, 1);
            for (int value : replicaMap.values()) {
                sum += value;
            }
            sum /= numValues;
            return sum;
        }
    }

    public void clear() {
        synchronized (replicaMap) {
            replicaMap.clear();
        }
    }
}
