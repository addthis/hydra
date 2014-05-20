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

import com.addthis.hydra.data.query.Query;
import com.addthis.meshy.ChannelMaster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultTaskAllocators {

    private static final Logger log = LoggerFactory.getLogger(DefaultTaskAllocators.class);

    private static final String DEFAULT_ALLOCATOR = Parameter.value("hydra.query.tasks.allocator", "balanced");

    private static final TaskAllocator PARALLEL_ALLOCATOR = new ParallelAllocator();
    private static final TaskAllocator PER_QUERY_RR_ALLOCATOR = new PerQueryRRAllocator();
    private static final TaskAllocator LAZY_ALLOCATOR = new LazyAllocator();

    private final BalancedAllocator balancedAllocator;

    public DefaultTaskAllocators(BalancedAllocator balanced_allocator) {
        balancedAllocator = balanced_allocator;
    }

    public void allocateQueryTasks(Query query, QueryTaskSource[] taskSources, ChannelMaster meshy,
            Map<String, String> queryOptions) {

        String queryAllocator = query.getParameter("allocator", DEFAULT_ALLOCATOR).toLowerCase();

        switch (queryAllocator) {
            case "parallel":
                PARALLEL_ALLOCATOR.allocateTasks(taskSources, meshy, queryOptions);
                break;
            case "lazy":
                LAZY_ALLOCATOR.allocateTasks(taskSources, meshy, queryOptions);
                break;
            case "balanced":
                balancedAllocator.allocateTasks(taskSources, meshy, queryOptions);
                break;
            case "legacy":
            default:
                PER_QUERY_RR_ALLOCATOR.allocateTasks(taskSources, meshy, queryOptions);
                break;
        }

    }
}
