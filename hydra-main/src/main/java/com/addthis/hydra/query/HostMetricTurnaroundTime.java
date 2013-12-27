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
package com.addthis.hydra.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.TreeMultimap;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * Implements the HostMetric interface and calculate the scores for hosts involved in a query based on their
 * turnaround time.
 */
public class HostMetricTurnaroundTime implements HostMetric {

    /**
     * The logger
     */
    private static final Logger log = LoggerFactory.getLogger(HostMetricTurnaroundTime.class);

    private double multipleStdDevs;

    private final HostMetadataTracker myMetadataTracker;

    public HostMetricTurnaroundTime(HostMetadataTracker tracker, double multipleStdDevs) {
        this.multipleStdDevs = multipleStdDevs;
        myMetadataTracker = tracker;
    }

    /**
     * Returns the metadata tracker used by this metric
     *
     * @return HostMetadataTracker a reference to the meta data tracker used by this metric
     */
    @Override
    public HostMetadataTracker getMetadataTracker() {
        return myMetadataTracker;
    }

    /**
     * Returns a HashMap containing the scores of the hosts. The slowest *normal* host gets a score of 1.0
     * and the fastest gets 0. If the host was a straggler, its score is set to HostMetric.STRAGGLER.
     *
     * @return a HashMap of the host name and its score
     */
    @Override
    public HashMap<String, Double> getHostsScores(String queryID) {
        HashMap<String, Double> scores = new HashMap<>();
        List<String> hostNames = myMetadataTracker.getHostNamesInQuery(queryID);
        if (hostNames == null || hostNames.size() == 0) {
            // Unknown query ID
            return null;
        }

        // Order the hosts by their slowness and assign a slowness score for each starting from 0 to 1.0
        TreeMultimap<Long, String> hostsDeltaTime = TreeMultimap.create();
        double sum = 0;
        double sumSquared = 0;
        double numberOfSamples = 0;

        synchronized (hostNames) {
            for (String hostName : hostNames) {
                Long deltaTime = myMetadataTracker.getHostDeltaTime(queryID, hostName);
                if (deltaTime == null && !myMetadataTracker.isRusher(queryID, hostName)) {
                    // That's not a rusher and didn't finish then it is a straggler
                    scores.put(hostName, HostMetric.STRAGGLER);
                    continue;
                }

                if (deltaTime != null) {
                    sum += deltaTime;
                    sumSquared += deltaTime * deltaTime;
                    numberOfSamples++;
                    hostsDeltaTime.put(deltaTime, hostName);
                }

                // Else this host is a rusher
            }
        }

        if (numberOfSamples == 0) {
            // We had one host only and it is a straggler
            return scores;
        }

        // n cannot be 0 from here on....
        // Now mark the non-stragglers but slow hosts
        double mean = sum / numberOfSamples;
        double variance = (sumSquared - 2 * sum * mean + numberOfSamples * mean * mean) / numberOfSamples;
        double stdDevsAway = mean + multipleStdDevs * Math.sqrt(variance);

        if (log.isTraceEnabled()) {
            log.trace("mean:" + mean + " stddev:" + Math.sqrt(variance) + " multiple:" + multipleStdDevs + " stdDevsAway:" + stdDevsAway);
        }

        ArrayList<Long> keysToDelete = new ArrayList<>();

        for (Map.Entry<Long, String> e : hostsDeltaTime.entries()) {
            if (e.getKey() > stdDevsAway) {
                keysToDelete.add(e.getKey());
                scores.put(e.getValue(), HostMetric.SLOW_HOST);
                if (log.isTraceEnabled()) {
                    log.trace("Marking host:" + e.getValue() + " as slow");
                }
            }
        }

        // Remove the hosts that needs to be remove
        for (Long key : keysToDelete) {
            hostsDeltaTime.removeAll(key);
        }

        // If we have only one host to determine score for, then put 0.5
        if (hostsDeltaTime.size() == 1) {
            // Only one host responded
            scores.put(hostsDeltaTime.values().iterator().next(), 0.5);
            return scores;
        }

        // The number of segments is 1 less than the number of hosts to score. So if we have 3 hosts to occupy
        // the score range from 0 -> 1, then we have two segments and their scores should be 0, 0.5 and 1.
        double segmentSize = 1.0 / (hostsDeltaTime.size() - 1);

        // Since the data is sorting ascending in turnaround time, the first we meet are the fastest hosts
        // and so they should get the lowest slowness score
        double score = 0;
        for (Map.Entry<Long, String> e : hostsDeltaTime.entries()) {
            scores.put(e.getValue(), score);
            score += segmentSize;
        }

        return scores;
    }
}
