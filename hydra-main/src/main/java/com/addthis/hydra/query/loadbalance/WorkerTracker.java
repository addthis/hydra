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

package com.addthis.hydra.query.loadbalance;

import java.util.Collection;

import com.addthis.basis.util.Parameter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class WorkerTracker {

    // should be 'arbitrarily high' -- high enough that two frame-readers are almost certainly going to
    // refer to the same WorkerData on request
    public static final int DEFAULT_CACHE_SIZE = Parameter.intValue("hydra.query.loadbalance.cache.size", 500);

    // maximum query tasks allowed per discovered query worker for this query master
    public static final int DEFAULT_WORKER_LEASES = Parameter.intValue("hydra.query.tasks.max", 3);

    final LoadingCache<String, WorkerData> workerCache;

    public WorkerTracker() {
        this.workerCache = CacheBuilder.newBuilder()
                .maximumSize(DEFAULT_CACHE_SIZE)
                .build(new CacheLoader<String, WorkerData>() {
                    @Override
                    public WorkerData load(String key) throws Exception {
                        return new WorkerData(key, DEFAULT_WORKER_LEASES);
                    }
                });
    }

    public Collection<WorkerData> values() {
        return workerCache.asMap().values();
    }

    public WorkerData get(String key) {
        return workerCache.getUnchecked(key);
    }
}
