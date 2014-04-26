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
package com.addthis.hydra.query.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * Stores the query meta data. Those include which hosts are involved in executing a certain query, how much time
 * did each take, the number of lines each returned and whether or not they are rushers (ran after detecting a
 * straggler).
 */
public class HostMetadataTracker {

    /**
     * Used to write to the log
     */
    private final Logger log = LoggerFactory.getLogger(HostMetadataTracker.class);

    /**
     * Stores a set of hosts running parts of a certain query
     */
    private final ConcurrentHashMap<String, List<String>> hostsInQueries =
            new ConcurrentHashMap<>();

    /**
     * End time query metrics
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> queryHostsDeltaTime =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>();

    /**
     * Number of lines query metrics
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> queryHostsLines =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>();

    /**
     * Stores whether or not a host is a rusher (those hosts that we run to catch up with the stragglers' queries)
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Boolean>> queryHostsRushers =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, Boolean>>();

    /**
     * Stores the number of queries per host
     */
    private final ConcurrentHashMap<String, AtomicInteger> queriesPerHost = new ConcurrentHashMap<String, AtomicInteger>();

    public HostMetadataTracker() {
    }

    /**
     * Returns the number of uncommitted queries
     *
     * @return int
     */
    public int getNumberOfUncomittedQueries() {
        return hostsInQueries.size();
    }

    /**
     * Marks the host as a rusher, which was run due to straggler. Due to host slowness, the
     * mesh query master might resort to sending the query to all possible hosts searching for an
     * answer as soon as possible. If a host was the cause, we assign the highest
     * possible slowness to that host if it never responded with the result before the query ends in an attempt to give
     * it a chance to finish its load, garbage collection or any other cause that's slowing it.
     * <p/>
     * This function marks this host as one of those backup rushers and so it will not get penalized at the commit
     * if it did not finish on time. The only hosts that are not marked as rushers and did not finish by the end
     * of the query are stragglers
     *
     * @param queryID  A String containing the ID of the query we attempted to run
     * @param hostName A String containing the host name
     */
    public void setHostAsRusher(String queryID, String hostName) {
        ConcurrentHashMap<String, Boolean> rushers = new ConcurrentHashMap<String, Boolean>();
        ConcurrentHashMap<String, Boolean> oldRushers = queryHostsRushers.putIfAbsent(queryID, rushers);

        if (oldRushers != null) {
            rushers = oldRushers;
        }

        rushers.put(hostName, true);
    }

    /**
     * Returns true if the host was run as a rusher for a certain query
     *
     * @param queryID  the id of the query
     * @param hostName the hostname to check
     * @return true if the host is a rusher otherwise false
     */
    public boolean isRusher(String queryID, String hostName) {
        ConcurrentHashMap<String, Boolean> rushers = queryHostsRushers.get(queryID);
        if (rushers == null) {
            return false; // No rushers for this query
        }
        Boolean isRusher = rushers.get(hostName);
        if (isRusher == null) {
            return false; // Was not on the rushers list
        }
        return isRusher;
    }


    /**
     * Sets the time a host starts running a query.
     *
     * @param queryID  A string identifying the ID of the query that will be (or just have been) executed
     * @param hostName A string containing the hostname
     */
    public void addHostToQuery(String queryID, String hostName) {
        List<String> hostsInQuery = Collections.synchronizedList(new ArrayList<String>());
        List<String> oldHostsInQuery = hostsInQueries.putIfAbsent(queryID, hostsInQuery);

        if (oldHostsInQuery != null) {
            hostsInQuery = oldHostsInQuery;
        }

        hostsInQuery.add(hostName);

        // Increase the number of queries running on that host
        AtomicInteger queryCount = queriesPerHost.get(hostName);
        if (queryCount == null) {
            queryCount = new AtomicInteger();
            AtomicInteger oldValue = queriesPerHost.putIfAbsent(hostName, queryCount);
            if (oldValue != null) {
                // Someone else inserted an AtomicInteger
                queryCount = queriesPerHost.get(hostName);
            }
        }
        queryCount.incrementAndGet();
    }

    /**
     * Decrements a host in the query histogram
     *
     * @param hostName A string containing the hostname
     */
    public void decrementAHostInQueryHistogram(String hostName) {
        // Increase the number of queries running on that host
        AtomicInteger queryCount = queriesPerHost.get(hostName);
        if (queryCount != null && queryCount.get() > 0) {
            queryCount.decrementAndGet();
        }
    }

    /**
     * Sets the time a host took to execute a query.
     *
     * @param queryID   A String containing the ID of the query that has just completed
     * @param hostName  A String containing the host name of the host that just finished running this query
     * @param deltaTime A long containing the difference between the end and start time
     */
    public void addHostDeltaTime(String queryID, String hostName, long deltaTime) {
        if (queryID == null) {
            log.warn("ERROR: Null queryID, not updating delta time");
            return;
        }
        ConcurrentHashMap<String, Long> hostDeltaTime = new ConcurrentHashMap<String, Long>();
        ConcurrentHashMap<String, Long> oldHostDeltaTime = queryHostsDeltaTime.putIfAbsent(queryID, hostDeltaTime);

        if (oldHostDeltaTime != null) {
            hostDeltaTime = oldHostDeltaTime;
        }

        boolean success;
        do {
            Long currentDeltaTime = hostDeltaTime.get(hostName);
            long newDeltaTime;
            if (currentDeltaTime == null) {
                newDeltaTime = deltaTime;
                success = (hostDeltaTime.putIfAbsent(hostName, newDeltaTime) == null);
            } else {
                newDeltaTime = currentDeltaTime + deltaTime;
                success = hostDeltaTime.replace(hostName, currentDeltaTime, newDeltaTime);
            }
        } while (!success);
    }

    /**
     * Sets the number of lines returned by query from a host.
     *
     * @param queryID  A string containing the query id
     * @param hostName A string containing the host name
     * @param lines    A long containing the number of lines
     */
    public void addHostLines(String queryID, String hostName, long lines) {
        if (queryID == null) {
            log.warn("ERROR: Null queryID, not updating host lines");
            return;
        }

        ConcurrentHashMap<String, Long> hostLines = new ConcurrentHashMap<String, Long>();
        ConcurrentHashMap<String, Long> oldHostLines = queryHostsLines.putIfAbsent(queryID, hostLines);

        if (oldHostLines != null) {
            hostLines = oldHostLines;
        }

        boolean success;
        do {
            Long currentLines = hostLines.get(hostName);
            long newLines;
            if (currentLines == null) {
                newLines = lines;
                success = (hostLines.putIfAbsent(hostName, newLines) == null);
            } else {
                newLines = currentLines + lines;
                success = hostLines.replace(hostName, currentLines, newLines);
            }
        } while (!success);
    }

    /**
     * Delete all the uncommitted metrics associated with a query. This happens if we start tracking some metrics
     * of a query, and it gets cancelled for some reason.
     *
     * @param queryID The query ID for which to purge metrics
     */
    public void deleteQueryMetrics(String queryID) {
        // Decrease the number of queries per host for the list of hosts running queryID
        List<String> hostNames = hostsInQueries.remove(queryID);
        queryHostsDeltaTime.remove(queryID);
        queryHostsLines.remove(queryID);
        queryHostsRushers.remove(queryID);

        synchronized (hostNames) {
            if (hostNames != null) {
                for (String hostName : hostNames) {
                    decrementAHostInQueryHistogram(hostName);
                }
            }
        }
    }

    /**
     * Returns the time a host took to execute a query.
     *
     * @param queryID  a String containing the query ID
     * @param hostName a String containing the host name
     * @return a Long containing the delta time or null if there is no delta time for that host or the queryID
     *         does not exist
     */
    public Long getHostDeltaTime(String queryID, String hostName) {
        ConcurrentHashMap<String, Long> hostTimes = queryHostsDeltaTime.get(queryID);
        if (hostTimes == null) {
            return null;
        } else {
            return hostTimes.get(hostName);
        }
    }

    /**
     * Returns the number of lines a host returned in a certain query
     *
     * @param queryID  The query ID
     * @param hostName The host name
     * @return a Long containing the number of lines returned by that host or null if either the host was not part of
     *         this query, or no such query exists
     */
    public Long getHostLines(String queryID, String hostName) {
        ConcurrentHashMap<String, Long> hostLines = queryHostsLines.get(queryID);
        if (hostLines == null) {
            return null;
        } else {
            return hostLines.get(hostName);
        }
    }

    /**
     * Returns a set containing the host names working in a certain query
     *
     * @param queryID The query id for which to return the list of hosts
     * @return a set containing Strings of host names or null if no such query exists
     */
    public List<String> getHostNamesInQuery(String queryID) {
        List<String> hostsInQuery = hostsInQueries.get(queryID);

        return (hostsInQuery == null) ? null : hostsInQuery;
    }

    /**
     * Returns a snapshot of the HashMap containing the queries per host
     *
     * @return ConcurrentHashMap containing the count of queries per host
     */
    public HashMap<String, Integer> getQueriesPerHostList() {
        HashMap<String, Integer> queriesPerHostSnapshot = new HashMap<>();
        for (Map.Entry<String, AtomicInteger> e : queriesPerHost.entrySet()) {
            queriesPerHostSnapshot.put(e.getKey(), e.getValue().intValue());
        }
        return queriesPerHostSnapshot;
    }
}
