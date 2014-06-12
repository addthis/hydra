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

import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.query.aggregate.DetailedStatusTask;

import io.netty.util.concurrent.Promise;

public class QueryEntry {

    final Query query;
    final AtomicInteger preOpLines;
    final AtomicInteger postOpLines;
    final int waitTime;
    final String queryDetails;
    final String[] opsLog;
    final TrackerHandler trackerHandler;

    long runTime;
    long startTime;

    QueryEntry(Query query, String[] opsLog, TrackerHandler trackerHandler) {
        this.query = query;
        this.opsLog = opsLog;
        this.trackerHandler = trackerHandler;
        this.preOpLines = new AtomicInteger();
        this.postOpLines = new AtomicInteger();
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

    void queryStart() {
        this.startTime = System.currentTimeMillis();
    }

    public QueryEntryInfo getStat() {
        QueryEntryInfo stat = new QueryEntryInfo();
        stat.paths = query.getPaths();
        stat.uuid = query.uuid();
        stat.ops = opsLog;
        stat.job = query.getJob();
        stat.alias = query.getParameter("track.alias");
        stat.sources = query.getParameter("sources");
        stat.remoteip = query.getParameter("remoteip");
        stat.sender = query.getParameter("sender");
        stat.lines = preOpLines.get();
        stat.runTime = getRunTime();
        stat.startTime = startTime;
//        stat.tasks = hostInfoSet;
        return stat;
    }

    long getRunTime() {
        return (runTime > 0) ? runTime : (System.currentTimeMillis() - startTime);
    }

    @Override
    public String toString() {
        return "QT:" + query.uuid() + ":" + startTime + ":" + runTime + " lines: " + preOpLines;
    }

    /**
     * cancels query if it's still running
     * otherwise, it's a null-op.
     */
    public boolean cancel() {
        // boolean parameter to cancel is ignored
        boolean success = false;
        success |= trackerHandler.queryPromise.cancel(false);
        success |= trackerHandler.opPromise.cancel(false);
        success |= trackerHandler.requestPromise.cancel(false);
        return success;
    }

    boolean tryFailure(Throwable cause) {
        // boolean parameter to cancel is ignored
        boolean success = false;
        success |= trackerHandler.queryPromise.tryFailure(cause);
        success |= trackerHandler.opPromise.tryFailure(cause);
        success |= trackerHandler.requestPromise.tryFailure(cause);
        return success;
    }

    public void getDetailedQueryEntryInfo(Promise<QueryEntryInfo> promise) {
        DetailedStatusTask task = new DetailedStatusTask(promise, this);
        trackerHandler.submitDetailedStatusTask(task);
    }

}
