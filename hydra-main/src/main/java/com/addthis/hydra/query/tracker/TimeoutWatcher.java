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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TimeoutWatcher implements Runnable {

    static final Logger log = LoggerFactory.getLogger(TimeoutWatcher.class);

    ConcurrentMap<String, QueryEntry> running;

    TimeoutWatcher(ConcurrentMap<String, QueryEntry> running) {
        this.running = running;
    }

    @Override
    public void run() {
        long currentTime = System.currentTimeMillis();
        for (QueryEntry queryEntry : running.values()) {
            if (queryEntry.waitTime <= 0) {
                continue;
            }
            long queryDuration = currentTime - queryEntry.startTime;
            // wait time is in seconds
            double queryDurationInSeconds = queryDuration / 1000.0;
            if (queryDurationInSeconds < queryEntry.waitTime) {
                log.info("query: {} running for: {} timeout is: {}",
                        queryEntry.query.uuid(), queryDurationInSeconds, queryEntry.waitTime);
            } else {
                log.warn("QUERY TIMEOUT query: {} running for: {} timeout is: {}",
                        queryEntry.query.uuid(), queryDurationInSeconds, queryEntry.waitTime);
                // sanity check duration
                if (queryDurationInSeconds > (2 * queryEntry.waitTime)) {
                    log.warn("query: {} query duration was insane, resetting to waitTime for logging. startTime: {}",
                            queryEntry.query.uuid(), queryEntry.startTime);
                }
                sendTimeout(queryEntry, queryEntry.waitTime);
            }
        }
    }

     static void sendTimeout(QueryEntry entry, long timeout) {
        String message = "[timeout.watcher] timeout: " + timeout +
                         " has been exceeded, canceling query: " + entry.query.uuid();
        if (entry.queryPromise == null) {
            log.error("QUERY TIMEOUT FAILURE: query: {} had a null query promise", entry.query.uuid());
        } else {
            if (!entry.queryPromise.tryFailure(new TimeoutException(message))) {
                log.warn("QUERY TIMEOUT FAILURE: query: {} could not fail promise {}",
                        entry.query.uuid(), entry.queryPromise);
            }
        }
    }
}
