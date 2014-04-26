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

package com.addthis.hydra.query.tracker;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryException;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.query.util.HostEntryInfo;
import com.addthis.hydra.util.StringMapHelper;

class QueryEntry {

     QueryTracker queryTracker;
     final Query query;
     final AtomicInteger lines;
     final AtomicBoolean finished = new AtomicBoolean(false);
     final HashSet<HostEntryInfo> hostInfoSet = new HashSet<>();
     final int waitTime;
     final String queryDetails;
    // Stores a reference to the queryopprocessor, which will be used in case the query gets closed
     final QueryOpProcessor queryOpProcessor;
     final String[] opsLog;

     volatile long runTime;
     volatile long startTime;
     volatile QueryHandle queryHandle;


    QueryEntry(QueryTracker queryTracker, Query query, QueryOpProcessor queryOpProcessor, String[] opsLog) {
        this.queryTracker = queryTracker;
        this.query = query;
        this.queryOpProcessor = queryOpProcessor;
        this.opsLog = opsLog;
        this.lines = new AtomicInteger();
        this.queryDetails = query.getJob() + "--" +
                            (query.getOps() == null ? "" : query.getOps());

        final String timeoutInSeconds = query.getParameter("timeout");
        this.startTime = System.currentTimeMillis();
        if (timeoutInSeconds != null) {
            waitTime = Integer.parseInt(timeoutInSeconds);
        } else {
            waitTime = -1;
        }
    }

     void queryStart(QueryHandle queryHandle) {
        this.queryHandle = queryHandle;
        this.startTime = System.currentTimeMillis();
    }

    public QueryEntryInfo toStat() {
        QueryEntryInfo stat = new QueryEntryInfo();
        stat.paths = query.getPaths();
        stat.uuid = query.uuid();
        stat.ops = opsLog;
        stat.job = query.getJob();
        stat.alias = query.getParameter("track.alias");
        stat.sources = query.getParameter("sources");
        stat.remoteip = query.getParameter("remoteip");
        stat.sender = query.getParameter("sender");
        stat.lines = lines.get();
        stat.runTime = getRunTime();
        stat.startTime = startTime;
        stat.hostInfoSet = hostInfoSet;
        return stat;
    }

     long getRunTime() {
        return (runTime > 0) ? runTime : (System.currentTimeMillis() - startTime);
    }

    public boolean addHostEntryInfo(HostEntryInfo newHostEntry) {
        return hostInfoSet.add(newHostEntry);
    }

    @Override
    public String toString() {
        return "QT:" + query.uuid() + ":" + startTime + ":" + runTime + " lines: " + lines;
    }

    /**
     * cancels query if it's still running
     * otherwise, it's a null-op.
     */
    public void cancel(QueryException reason) {
        reason = reason == null ? new QueryException("client cancel") : reason;
        QueryEntry queryEntry = finish(reason);

        if (queryEntry != null && queryEntry.queryHandle != null) {
            queryEntry.queryHandle.cancel(reason.getMessage());
        } else {
            QueryTracker.log.warn("Unable to cancel query because queryEntry could be null : {}", queryEntry);
        }
    }

    public QueryEntry finish(DataChannelError error) {
        if (!finished.compareAndSet(false, true)) {
            return null;
        }

        runTime = getRunTime();

        QueryEntry runE = queryTracker.running.remove(query.uuid());
        if (runE == null) {
            QueryTracker.log.warn("failed to remove running for {}", query.uuid());
            return null;
        }

        queryTracker.queryGate.release();

        // Finish will be called in case of source error. We need to make sure to close all operators as some
        // might hold non-garbage collectible resources like BDB in gather
        try {
            if (queryOpProcessor != null) {
                queryOpProcessor.close();
            }
        } catch (Exception e) {
            QueryTracker.log.warn("Error while closing queryOpProcessor", e);
        }

        try {
            // If a query never started (generally due to an error with file references) don't record its runtime
            if (startTime > 0) {
                queryTracker.queryMeter.update(runTime, TimeUnit.MILLISECONDS);
            }

            if (error != null) {
                queryTracker.log(new StringMapHelper()
                        .put("type", "query.error")
                        .put("query.path", query.getPaths()[0])
                        .put("query.ops", Arrays.toString(opsLog))
                        .put("sources", query.getParameter("sources"))
                        .put("time", System.currentTimeMillis())
                        .put("time.run", runTime)
                        .put("job.id", query.getJob())
                        .put("job.alias", query.getParameter("track.alias"))
                        .put("query.id", query.uuid())
                        .put("lines", lines)
                        .put("sender", query.getParameter("sender"))
                        .put("error", error.getMessage()));
                queryTracker.queryErrors.inc();
                return runE;
            }

            queryTracker.recentlyCompleted.put(query.uuid(), runE.toStat());

            StringMapHelper queryLine = new StringMapHelper()
                    .put("type", "query.done")
                    .put("query.path", query.getPaths()[0])
                    .put("query.ops", Arrays.toString(opsLog))
                    .put("sources", query.getParameter("sources"))
                    .put("time", System.currentTimeMillis())
                    .put("time.run", runTime)
                    .put("job.id", query.getJob())
                    .put("job.alias", query.getParameter("track.alias"))
                    .put("query.id", query.uuid())
                    .put("lines", lines)
                    .put("sender", query.getParameter("sender"));
            queryTracker.log(queryLine);
        } catch (Exception e) {
            QueryTracker.log.warn("Error while doing record keeping for a query.", e);
        }
        return runE;
    }
}
