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
public class HostMetricRowsPerSecond implements HostMetric {

    /**
     * The logger
     */
    private static final Logger log = LoggerFactory.getLogger(HostMetricRowsPerSecond.class);

    private double multipleStdDevs;

    HostMetadataTracker myMetadataTracker;

    public HostMetricRowsPerSecond(HostMetadataTracker tracker, double multipleStdDevs) {
        myMetadataTracker = tracker;
        this.multipleStdDevs = multipleStdDevs;
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
        HashMap<String, Double> scores = new HashMap<String, Double>();
        List<String> hostNames = myMetadataTracker.getHostNamesInQuery(queryID);
        if (hostNames == null || hostNames.size() == 0) {
            // Unknown query ID
            return null;
        }

        TreeMultimap<Double, String> hostTimePerLine = TreeMultimap.create();
        double sum = 0;
        double sumSquared = 0;
        double numberOfSamples = 0;

        synchronized (hostNames) {
            for (String hostName : hostNames) {
                Long deltaTime = myMetadataTracker.getHostDeltaTime(queryID, hostName);
                if (deltaTime == null) {
                    if (myMetadataTracker.isRusher(queryID, hostName)) {
                        // Don't penalize this host.
                        continue;
                    }

                    // That's not a rusher, then it is a straggler
                    scores.put(hostName, HostMetric.STRAGGLER);
                    continue;
                }

                Long lines = myMetadataTracker.getHostLines(queryID, hostName);
                if (lines == null) {
                    log.warn("ERROR: Lines cannot be null at this point. QueryID: " + queryID + " Host: " + hostName +
                             ". Not updating this host speed");
                    continue;
                }

                double timePerLine = 1.0 * lines / deltaTime;

                sum += timePerLine;
                sumSquared += timePerLine * timePerLine;
                numberOfSamples++;

                // The 1 and the 10 provide sample smoothing to avoid dividing by 0. So if you put 0 in both lines
                // and deltaTime the default value of the lines per millisecond is 1 line / 10 milliseconds .
                hostTimePerLine.put(timePerLine, hostName);
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

        ArrayList<Double> keysToDelete = new ArrayList<>();

        for (Map.Entry<Double, String> e : hostTimePerLine.entries()) {
            if (e.getKey() > stdDevsAway) {
                keysToDelete.add(e.getKey());
                scores.put(e.getValue(), HostMetric.SLOW_HOST);
            }
        }

        // Remove the hosts that needs to be remove
        for (Double key : keysToDelete) {
            hostTimePerLine.removeAll(key);
        }

        if (hostTimePerLine.size() == 1) {
            // Only one host responded
            scores.put(hostTimePerLine.values().iterator().next(), 0.5);
            return scores;
        }

        double segmentSize = 1.0 / (hostTimePerLine.size() - 1);
        double score = 0;
        for (Map.Entry<Double, String> e : hostTimePerLine.entries()) {
            scores.put(e.getValue(), score);
            score += segmentSize;
        }

        return scores;
    }
}
