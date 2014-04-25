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

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.addthis.basis.util.JitterClock;

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.query.util.QueryData;

class StragglerCheckThread extends Thread {

    ConcurrentHashMap<String, QueryWatcher> activeQueryMap = new ConcurrentHashMap<String, QueryWatcher>();

    public StragglerCheckThread() {
        setDaemon(true);
        setName("StragglerCheckThread");
        start();
    }

    public void addQuery(Query query, AggregateHandle handle, MeshSourceAggregator meshSourceAggregator) {
        activeQueryMap.put(query.uuid(), new QueryWatcher(query, handle, meshSourceAggregator));

    }

    public void removeQuery(Query query) {
        activeQueryMap.remove(query.uuid());
    }

    @Override
    public void run() {
        while (!MeshSourceAggregator.exiting.get()) {
            for (QueryWatcher queryWatcher : activeQueryMap.values()) {
                boolean isStraggler;
                if (MeshSourceAggregator.useStdDevStragglers) {
                    isStraggler = checkForStragglers(queryWatcher);
                } else {
                    isStraggler = checkForStragglersLegacy(queryWatcher);
                }

                if (isStraggler) {
                    activeQueryMap.remove(queryWatcher.query.uuid());
                }
            }

            try {
                Thread.sleep(MeshSourceAggregator.stragglerCheckPeriod);
            } catch (InterruptedException e) {
                MeshSourceAggregator.log.warn("Straggler checker was interrupted; exiting");
            }
        }
    }

    boolean checkForStragglers(QueryWatcher queryWatcher) {
        AggregateHandle handle = queryWatcher.handle;
        Query query = queryWatcher.query;

        if (handle.done.get()) {
            return true;
        }

        int totalTasks = handle._totalTasks;
        int numRemaining = totalTasks - handle.completed.size();
        int tasksDoneCutoff = Math.max(1, (int) Math.ceil(MeshSourceAggregator.stragglerCheckHostPercentage * totalTasks));
        long elapsedTime = JitterClock.globalTime() - handle.getStartTime();
        double timeCutoff = MeshSourceAggregator.stragglerCheckMeanRuntimeFactor * handle.getMeanRuntime();

        if (numRemaining == 0) {
            if (MeshSourceAggregator.log.isDebugEnabled() || query.isTraced()) {
                Query.emitTrace("Straggler checker for " + query.uuid() + " detected query done. Exiting.");
            }
            return true;
        } else if (numRemaining <= tasksDoneCutoff && elapsedTime > handle.getStdDevsAwayRuntime(MeshSourceAggregator.MULTIPLE_STD_DEVS)) {
            if (MeshSourceAggregator.log.isTraceEnabled()) {
                MeshSourceAggregator.log.trace("Running stragglers for query: " + query.uuid());
                MeshSourceAggregator.log.trace("numRemaining: " + numRemaining +
                         " taskDoneCutoff: " + tasksDoneCutoff +
                         " deltaTime: " + elapsedTime +
                         " " + MeshSourceAggregator.MULTIPLE_STD_DEVS + " stdDevsAway: " + handle.getStdDevsAwayRuntime(MeshSourceAggregator.MULTIPLE_STD_DEVS) +
                         " Mean runtime: " + handle.getMeanRuntime());
            }

            for (int node = 0; node < totalTasks; node++) {
                if (!handle.isComplete(node) && !handle.isStarted(node)) {
                    synchronized (handle.sourcesByTaskID) {
                        Set<QueryData> queryDataSet = handle.sourcesByTaskID.get(node);
                        if (queryDataSet != null && queryDataSet.size() > 0) {
                            Iterator<QueryData> iterator = queryDataSet.iterator();
                            if (iterator.hasNext()) {
                                QueryData queryData = iterator.next();
                                MeshSourceAggregator.totalStragglerCheckerRequests.inc();
                                iterator.remove();
                                String id = queryWatcher.meshSourceAggregator.requestQueryData(queryData, query);
                                if (MeshSourceAggregator.log.isDebugEnabled() || query.isTraced()) {
                                    MeshSourceAggregator.log.warn("Straggler detected for " + query.uuid() + " node " + node + " sending duplicate query to host: " + queryData.hostEntryInfo.getHostName() + " sourceId: " + id);
                                }
                            }
                        }
                    }
                }
            }
        }

        return false;
    }

    boolean checkForStragglersLegacy(QueryWatcher queryWatcher) {
        AggregateHandle handle = queryWatcher.handle;
        Query query = queryWatcher.query;
        if (handle.done.get()) {
            return true;
        }
        int totalTasks = handle._totalTasks;
        int numRemaining = totalTasks - handle.completed.size();
        int tasksDoneCutoff = Math.max(1, (int) Math.ceil(MeshSourceAggregator.stragglerCheckHostPercentage * totalTasks));
        long elapsedTime = JitterClock.globalTime() - handle.getStartTime();
        double timeCutoff = MeshSourceAggregator.stragglerCheckMeanRuntimeFactor * handle.getMeanRuntime();
        if (numRemaining == 0) {
            if (MeshSourceAggregator.log.isDebugEnabled() || query.isTraced()) {
                Query.emitTrace("Straggler checker for " + query.uuid() + " detected query done. Exiting.");
            }
            return true;
        } else if (numRemaining <= tasksDoneCutoff && (elapsedTime > timeCutoff)) {
            handle.handleStragglers();
        }
        return false;
    }
}
