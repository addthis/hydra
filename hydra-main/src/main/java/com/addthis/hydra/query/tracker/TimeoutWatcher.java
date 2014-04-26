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

import com.addthis.bundle.channel.DataChannelError;

class TimeoutWatcher extends Thread {

     QueryTracker queryTracker;

    TimeoutWatcher(QueryTracker queryTracker) {
        this.queryTracker = queryTracker;
        setName("QueryTimeoutWatcher");
        setDaemon(true);
        start();
    }

    @Override
    public void run() {
        try {
            long currentTime = System.currentTimeMillis();
            for (QueryEntry queryEntry : queryTracker.running.values()) {
                if (queryEntry.waitTime <= 0 || queryEntry.query == null) {
                    continue;
                }
                long queryDuration = currentTime - queryEntry.startTime;
                // wait time is in seconds
                double queryDurationInSeconds = queryDuration / 1000.0;
                QueryTracker.log.warn("[timeout.watcher] query: " + queryEntry.query.uuid() + " has been running for: " + queryDurationInSeconds + " timeout is: " + queryEntry.waitTime);
                if (queryDurationInSeconds > queryEntry.waitTime) {
                    QueryTracker.log.warn("[timeout.watcher] query: " + queryEntry.query.uuid() + " has been running for more than " + queryDurationInSeconds + " seconds.  Timeout:" + queryEntry.waitTime + " has been breached.  Canceling query");
                    // sanity check duration
                    if (queryDurationInSeconds > 2 * queryEntry.waitTime) {
                        QueryTracker.log.warn("[timeout.watcher] query: " + queryEntry.query.uuid() + " query duration was insane, resetting to waitTime.  startTime: " + queryEntry.startTime);
                    }
                    sendTimeout(queryEntry, queryEntry.waitTime);
                }
            }
            Thread.sleep(5000);
        } catch (Exception e) {
            QueryTracker.log.warn("[timeout.watcher] exception while watching queries: " + e.getMessage(), e);
        }
    }

     void sendTimeout(QueryEntry entry, long timeout) {
        String message = "[timeout.watcher] timeout: " + timeout + " has been exceeded, canceling query: " + (entry.query == null ? "" : entry.query.uuid());
        if (entry.queryHandle == null) {
            QueryTracker.log.warn("[timeout.watcher] Error.  query: " + entry.query.uuid() + " had a null query handle, removing from running list");
            entry.finish(new DataChannelError(message));

        } else {
            entry.queryHandle.cancel(message);
            if (queryTracker.running.containsKey(entry.query.uuid())) {
                QueryTracker.log.warn("[timeout.watcher] Error.  query: " + entry.query.uuid() + " was still on running list after canceling. Forcing finish");
                entry.finish(new DataChannelError(message));
            }
        }
    }
}
