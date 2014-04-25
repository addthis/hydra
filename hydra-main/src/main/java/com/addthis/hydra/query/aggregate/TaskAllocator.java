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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.query.util.QueryData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TaskAllocator {

    static final Logger log = LoggerFactory.getLogger(TaskAllocator.class);

    private TaskAllocator() {
    }

    /**
     * Allocate the query task to the best available host.  Currently we only try
     * to evenly distribute the queries amongst available hosts.  For example if hosts
     * 1 and 2 both have data for task a but host 1 already has a query task assigned
     * to it for a different task then we will pick host 2.
     *
     * @param queryPerHostCountMap - map of number of queries assigned to each host
     * @param queryDataSet         - the available queryData objects for the task being assigned
     * @param hostMap              - the map of host ids to a boolean describing whether the host is read only.
     */
    public static QueryData allocateQueryTaskLegacy(Map<String, Integer> queryPerHostCountMap,
            Set<QueryData> queryDataSet, Map<String, Boolean> hostMap) {
        if (queryDataSet == null || queryDataSet.size() == 0) {
            throw new RuntimeException("fileReferenceWrapper list cannot be empty");
        }
        SortedSet<QueryData> hostList = new TreeSet<>();
        int taskId = -1;
        for (QueryData queryData : queryDataSet) {
            taskId = queryData.taskId;
            String host = queryData.hostEntryInfo.getHostName();
            // initialize map
            if (queryPerHostCountMap.get(host) == null) {
                queryPerHostCountMap.put(host, 0);
            }
            hostList.add(queryData);
        }

        if (log.isTraceEnabled()) {
            log.trace("hostList size for task: " + taskId + " was " + hostList.size());
        }

        int minNumberAssigned = -1;
        QueryData bestQueryData = null;
        boolean readOnlyHostSelected = false;
        for (QueryData queryData : hostList) {
            String host = queryData.hostEntryInfo.getHostName();
            int numberAssigned = queryPerHostCountMap.get(host);
            if (log.isTraceEnabled()) {
                log.trace("host: " + host + " currently has: " + numberAssigned + " assigned");
            }
            boolean readOnlyHost = hostMap.containsKey(host) && hostMap.get(host);
            if (log.isTraceEnabled()) {
                log.trace(host + " readOnly:" + readOnlyHost);
            }
            // if we haven't picked any host
            // or if the host we are evaluating is a readOnly host
            // or if the current number of sub queries assigned to this host is less than the least loaded host we have found so far
            if (minNumberAssigned < 0 || (readOnlyHost && MeshSourceAggregator.prioritiseReadOnlyWorkers) || numberAssigned < minNumberAssigned) {
                // we only update the 'best' variable if one of two conditions is met:
                // 1 - we have not already selected a read only host
                // 2- we have selected a read only host AND the new host is read only and it has fewer sub-queries assigned to it
                if (!readOnlyHostSelected || (readOnlyHost && numberAssigned < minNumberAssigned)) {
                    minNumberAssigned = numberAssigned;
                    bestQueryData = queryData;
                    if (readOnlyHost) {
                        readOnlyHostSelected = true;
                    }
                }
            }
        }
        // bestWrapper should never be null here
        if (bestQueryData == null) {
            throw new QueryException("Unable to select best query data");
        }
        if (log.isTraceEnabled()) {
            log.trace("selected host: " + bestQueryData.hostEntryInfo.getHostName() + " as best host for task: " + bestQueryData.taskId);
        }
        queryPerHostCountMap.put(bestQueryData.hostEntryInfo.getHostName(), (queryPerHostCountMap.get(bestQueryData.hostEntryInfo.getHostName()) + 1));
        return bestQueryData;
    }
}
