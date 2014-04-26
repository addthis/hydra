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

import java.io.File;
import java.io.IOException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.RollingLog;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * HostSlownessTracker tries to track the <b>slowness</b> of the hosts running queries. The information it collects
 * gets used by the function pickAHost, which first avoids excessively slow hosts, then runs a roulette wheel
 * algorithm to randomly choose one host giving higher chances of fast hosts to get picked.
 * <p/>
 * It is assumed that only one instance of this class exists. That's why the constructor is private, and to obtain
 * a reference to that instance use the function getInstance() .
 */
public class HostSlownessTracker {

    /**
     * The logger
     */
    private final Logger log = LoggerFactory.getLogger(HostSlownessTracker.class);

    /**
     * Stores a floating point score representing how slow a host is between 0 to
     * {@link #maxNormalSlowness}*{@link #slowHostMultiplier}. The host slowness should asymptotically get nearer to
     * this value and gets reduced by the exponential decay from the {@link #decayTimeConstant}.
     * The slowness score is calculated as follows:
     * Score(S+)=Score(S)*exp+N*(1-exp)
     * where the exp = exp(-1/DecaySamples) and N is the new value.
     */
    private final ConcurrentHashMap<String, Double> hostSlowScore = new ConcurrentHashMap<String, Double>();

    /**
     * Stores a floating point score representing how slow a host is between 0 to 1.
     * The slowness score is calculated as follows:
     * Score(S+)=Score(S)*exp+N*(1-exp)
     * where the exp = exp(-1/DecaySamples) and N is the new value.
     */
    private final ConcurrentHashMap<String, Double> hostPreviousSlowScore = new ConcurrentHashMap<String, Double>();

    /**
     * Stores the last time a host has been updated
     */
    private final ConcurrentHashMap<String, Long> lastHostUpdate = new ConcurrentHashMap<String, Long>();

    /**
     * Stores the number of samples used to calculate the decay exponent
     */
    private int decaySamplesConstant = Parameter.intValue("slowness.decaySamplesConstant", 1);

    /**
     * Stores the score that should be assigned to the slowest server that did not cause us to issue stragglers
     */
    private int maxNormalSlowness = Parameter.intValue("slowness.maxNormalSlowness", 50);

    /**
     * Stores the score that should be assigned to the slowest server that did not cause us to issue stragglers
     */
    private double queriesWeight = Double.parseDouble(Parameter.value("slowness.queriesWeight", "0.5"));

    /**
     * Stores the multiplier by which we will multiply the {@link #maxNormalSlowness} and assign for slow hosts to
     * penalize them for being slow.
     */
    private int slowHostMultiplier = Parameter.intValue("slowness.slowHostMultiplier", 2);

    /**
     * Stores the calculated exponent calculated using the decaySamplesConstant
     */
    private double exp;

    /**
     * The time constant in milliseconds used to increase the slowness over time from the previous value. The attack
     * time acts as a "probation period", where the slowness of the hosts gets to smoothly increase to its goal giving
     * the host a chance to appear in queries and fix its score just in case this sample was a noisy extraneous one.
     */
    private int attackTimeConstant = Parameter.intValue("slowness.attackTimeConstant", 10000);

    /**
     * The time constant in milliseconds used to decay the slowness over time. Default is 120000
     */
    private int decayTimeConstant = Parameter.intValue("slowness.decayTimeConstant", 120000);

    /**
     * If a server has not been evaluated for speed, its speed is assigned a value of {@link #maxNormalSlowness}/initialHostSlownessQuotient
     */
    private double initialHostSlownessQuotient = Parameter.intValue("slowness.quotient", 4);

    /**
     * A rolling log of host slowness
     */
    private RollingLog slowHostLog;

    /**
     * Whether or not to use a rolling log to write the slowness data
     */
    private static final boolean writeSlownessLogs = Parameter.boolValue("slowness.log", false);

    /**
     * The metric used to score the metadata.
     */
    private final HostMetric hostMetric;

    /**
     * A gauge to keep track of the uncommitted queries
     */
    private final Gauge uncomittedQueriesGauge;

    /**
     * A gauge to keep track of the uncommitted queries
     */
    private final Gauge totalNumberOfHosts;

    /**
     * A meter to keep track of the number of queries
     */
    private final Meter numberOfQueries;

    /**
     * A meter to keep track of the number of Stragglers
     */
    private final Meter numberOfStragglers;

    /**
     * A gauge to keep track of the slow hosts
     */
    private final Gauge numberOfSlowHosts;

    /**
     * Constructor
     */
    public HostSlownessTracker(HostMetric metric) {
        String slownessLogDirectory = Parameter.value("slowness.LOG_DIR", "log/slowness");
        try {
            slowHostLog = new RollingLog(new File(slownessLogDirectory).getCanonicalFile(),
                    "host-slowness", null, true, 100 * 1024 * 1024, 1000 * 60 * 60);
        } catch (IOException e) {
            slowHostLog = null;
            log.warn("ERROR: could not create a rolling log in directory '" + slownessLogDirectory);
        }

        // Since we initialized the decaySamplesConstant from the system parameters, then we should update
        // the exponent here
        updateExponent();

        hostMetric = metric;

        // Yammer metrics

        // Create a gauge showing the number of uncommitted queries
        uncomittedQueriesGauge = Metrics.newGauge(HostSlownessTracker.class, "Uncommitted queries",
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return hostMetric.getMetadataTracker().getNumberOfUncomittedQueries();
                    }
                });

        // Create a gauge showing the number of uncommitted queries
        totalNumberOfHosts = Metrics.newGauge(HostSlownessTracker.class, "Total number of hosts",
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return hostSlowScore.size();
                    }
                });

        numberOfQueries = Metrics.newMeter(HostSlownessTracker.class,
                "Number of queries", "Number of queries", TimeUnit.SECONDS);

        numberOfStragglers = Metrics.newMeter(HostSlownessTracker.class,
                "Number of unfinished stragglers", "Number of unfinished stragglers", TimeUnit.SECONDS);

        numberOfSlowHosts = Metrics.newGauge(HostSlownessTracker.class, "Number of slow hosts",
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        int numberOfSlowHosts = 0;
                        long currentTime = System.currentTimeMillis();
                        for (Map.Entry<String, Double> e : hostSlowScore.entrySet()) {
                            double slowness = getHostSlowness(e.getKey(), currentTime);
                            if (slowness > maxNormalSlowness) {
                                numberOfSlowHosts++;
                            }
                        }
                        return numberOfSlowHosts;
                    }
                });

        // Should we log the data
        if (writeSlownessLogs) {
            logHostSlownessData();
        }
    }

    /**
     * returns the slow host multiplier {@link #slowHostMultiplier}
     *
     * @return an int containing the current {@link #slowHostMultiplier}
     */
    public int getSlowHostMultiplier() {
        return slowHostMultiplier;
    }

    /**
     * Sets the slow host multiplier {@link #slowHostMultiplier}
     */
    public void setSlowHostMultiplier(int slowHostMultiplier) {
        this.slowHostMultiplier = slowHostMultiplier;
    }

    /**
     * Returns the attack time constant
     *
     * @return an int containing the attack time constant
     * @see #attackTimeConstant
     */
    public int getAttackTimeConstant() {
        return attackTimeConstant;
    }

    /**
     * Sets the attack time constant
     *
     * @see #attackTimeConstant
     */
    public void setAttackTimeConstant(int attackTimeConstant) {
        this.attackTimeConstant = attackTimeConstant;
    }

    /**
     * Commits the metrics stored for a certain query. This function should be called after all the
     * {@link HostMetadataTracker#addHostToQuery(String, String)},
     * {@link HostMetadataTracker#addHostDeltaTime(String, String, long)}
     * and {@link HostMetadataTracker#addHostLines(String, String, long)}
     * have been called for every host running this query.
     *
     * @param queryID A String containing the ID of the query
     * @return A reference to the only instance of HostSlownessTracker to be reused again.
     */
    public HostSlownessTracker commitQueryHostMetrics(String queryID) {
        // Get the scores according to the metric that the user passed to us
        HashMap<String, Double> hostScores = hostMetric.getHostsScores(queryID);

        // Happens if the query is unknown
        if (hostScores == null) {
            return this;
        }

        /** Mark the {@link #numberOfQueries} metric */
        numberOfQueries.mark();

        for (Map.Entry<String, Double> e : hostScores.entrySet()) {
            String hostName = e.getKey();

            // Decrement this host query count in the histogram
//          hostMetric.getMetadataTracker().decrementAHostInQueryHistogram(hostName);

            double score = e.getValue();
            if (score == HostMetric.STRAGGLER || score == HostMetric.SLOW_HOST) {
                if (log.isTraceEnabled()) {
                    if (score == HostMetric.STRAGGLER) {
                        log.warn("Host: " + e.getKey() +
                                 " didn't finish & will be penalized as a cause for running stragglers.");
                    }

                    if (score == HostMetric.SLOW_HOST) {
                        log.warn("Host: " + e.getKey() +
                                 " was slower than the rest & will be penalized.");
                    }
                }

                if (score == HostMetric.STRAGGLER) {
                    numberOfStragglers.mark();
                }

                updateHostSlowness(hostName, slowHostMultiplier * maxNormalSlowness);
            } else {
                updateHostSlowness(hostName, score * maxNormalSlowness);
            }
        }

        return this;
    }

    /**
     * Returns the Decay Samples Constant
     */
    public int getDecaySamplesConstant() {
        return decaySamplesConstant;
    }

    /**
     * Sets the Decay Samples Constant
     */
    public void setDecaySamplesConstant(int decaySamplesConstant) {
        this.decaySamplesConstant = decaySamplesConstant;
        updateExponent();
    }

    /**
     * Returns the max possible normal slowness that a good server (non-straggler) can have.
     *
     * @return a double containing the maximum slowness
     * @see #maxNormalSlowness
     */
    public int getMaxNormalSlowness() {
        return maxNormalSlowness;
    }


    /**
     * Sets the maximum possible normal slowness that a good server (non-straggler) can have
     *
     * @param maxNormalSlowness a double containing the maximum slowness
     * @see #maxNormalSlowness
     */
    public void setMaxNormalSlowness(int maxNormalSlowness) {
        this.maxNormalSlowness = maxNormalSlowness;
    }

    /**
     * Returns the current time constant that controls the decay of slowness over time.
     *
     * @return a double containing the time constant
     */
    public int getDecayTimeConstant() {
        return decayTimeConstant;
    }

    /**
     * Sets the time constant that controls the decay of slowness over time.
     *
     * @param decayTimeConstant a double containing the time constant
     */
    public void setDecayTimeConstant(int decayTimeConstant) {
        this.decayTimeConstant = decayTimeConstant;
    }

    /**
     * Updates the exponent used in the decay sample averaging
     */
    private void updateExponent() {
        exp = Math.exp(-1.0 / decaySamplesConstant);
    }

    /**
     * Calculates the new value of the sample moving average using its decay exponent {@link #exp}, the new sample
     * value newSample and the current value currentValue of the sample moving average.
     *
     * @param newSample    a double containing the value of the new sample to be added to the moving average
     * @param currentValue a double containing the current value of the moving average
     * @return returns the new value of the moving average after adding the new sample
     */
    double calculateSampleMovingAverage(double newSample, double currentValue) {
        return currentValue * exp + newSample * (1 - exp);
    }

    /**
     * Updates a host slowness
     *
     * @param hostName a String containing the host name
     * @param slowness a number between 0 and {@link #maxNormalSlowness} * {@link #slowHostMultiplier}
     *                 with 0 being the fastest.
     */
    private void updateHostSlowness(String hostName, double slowness) {
        if (hostName == null) {
            return;
        }

        Double current = hostSlowScore.get(hostName);
        if (current == null) {
            current = 0.0;
        }

        hostPreviousSlowScore.put(hostName, getHostSlowness(hostName, System.currentTimeMillis()));
        hostSlowScore.put(hostName, calculateSampleMovingAverage(slowness, current));
        lastHostUpdate.put(hostName, System.currentTimeMillis());
    }

    /**
     * Returns a double representing the slowness of a host at a certain time instant identified by
     * currentTime. currentTime has always to be greater than or equal the last time the host slowness was updated.
     *
     * @param hostName    a String identifying the host name.
     * @param currentTime a long identifying the time instant at which to calculate the host slowness
     * @return a double representing the host slowness at that instant.
     */
    public double getHostSlowness(String hostName, long currentTime) {
        if (hostName == null) {
            return 0;
        }

        Double currentSlowness = hostSlowScore.get(hostName);
        if (currentSlowness == null) {
            return 1.0 * maxNormalSlowness / initialHostSlownessQuotient;    // New host
        }

        double lastSlowness = hostPreviousSlowScore.get(hostName);


        double timeDifference = currentTime - lastHostUpdate.get(hostName);
        if (timeDifference < 0) {
            timeDifference = 0;
        }

        double attack = (currentSlowness - lastSlowness) * (1 - Math.exp(-1.0 * timeDifference / attackTimeConstant));

        return (lastSlowness + attack) * Math.exp(-1.0 * timeDifference / decayTimeConstant);
    }

    String rouletteWheel(HashMap<String, Double> scores) {
        if (scores.size() == 0) {
            return null;
        }

        double total = 0;
        for (Map.Entry<String, Double> e : scores.entrySet()) {
            total += e.getValue();
        }

        double ball = Math.random() * total;
        total = 0;
        for (Map.Entry<String, Double> e : scores.entrySet()) {
            total += e.getValue();
            if (total >= ball) {
                return e.getKey();
            }
        }

        log.warn("ERROR: rouletteWheel: should not have reached this point in code");
        return scores.entrySet().iterator().next().getKey();
    }

    String maxOf(HashMap<String, Double> scores) {
        if (scores.size() == 0) {
            return null;
        }

        double max = -Double.MAX_VALUE;
        String maxKey = "";

        for (Map.Entry<String, Double> e : scores.entrySet()) {
            if (max < e.getValue()) {
                max = e.getValue();
                maxKey = e.getKey();
            }
        }

        if (max <= 0) {
            log.warn("ERROR: maxOf: should not have reached this point in code");
            return scores.entrySet().iterator().next().getKey();
        }

        return maxKey;
    }

    String pickAHostUsingCollectedStatistics(Set<String> hostNames, HashMap<String, Double> currentSlowness,
            double queriesWeight, boolean probabilisticSelection) {
        HashMap<String, Integer> queriesPerHost = hostMetric.getMetadataTracker().getQueriesPerHostList();

        // Find the minimum int
        HashMap<String, Double> hostScores = new HashMap<String, Double>();
        double maxPossibleSlowness = slowHostMultiplier * maxNormalSlowness;
        int maxQueries = 0;
        for (Map.Entry<String, Integer> query : queriesPerHost.entrySet()) {
            int queries = query.getValue();
            if (queries > maxQueries) {
                maxQueries = queries;
            }
        }

        for (String hostName : hostNames) {
            Integer numberOfQueries = queriesPerHost.get(hostName);
            if (numberOfQueries == null) {
                // The host is unknown, never got queries before
                numberOfQueries = 0;
            }

            double scaledSlowness = currentSlowness.get(hostName) / maxPossibleSlowness;
            double scaledHistogramQueries = 1.0 * numberOfQueries / (maxQueries + 0.1);

            hostScores.put(hostName, 1.0 / (queriesWeight * scaledHistogramQueries + (1.0 - queriesWeight) * scaledSlowness));
        }

        // Return the first choice. In case more than one host has the same number of queries, we just
        // pick the first one.
        if (probabilisticSelection) {
            return rouletteWheel(hostScores);
        } else {
            return maxOf(hostScores);
        }
    }

    /**
     * Given a set of hosts that can run a sub query, this function will use the historical data we have about those
     * hosts to pick a good choice. This function will randomly pick one of the good hosts using a roulette wheel
     * algorithm to avoid flooding the best host with queries.
     *
     * @param hostNamesSet A String Set containing a set of *UNIQUE* names of the hosts to pick from
     * @return a String containing the chosen host name.
     */
    public String pickAHost(Set<String> hostNamesSet) {
        // Clone the hostNameSet
        Set<String> hostNames = new HashSet<>();
        hostNames.addAll(hostNamesSet);

        if (hostNames == null || hostNames.isEmpty()) {
            return null;
        }

        /* If we have only one host, then there's no point in choosing, just return the one we have */
        if (hostNames.size() == 1) {
            return hostNames.iterator().next();
        }

        // Calculate the hosts slowness once and reuse it as needed
        HashMap<String, Double> currentSlowness = new HashMap<String, Double>();
        HashMap<String, Double> slowHosts = new HashMap<String, Double>();
        long currentTime = System.currentTimeMillis();
        double maxPossibleSlowness = slowHostMultiplier * maxNormalSlowness;
        double maxSlowness = -1;
        String slowestHost = "";
        for (String hostName : hostNames) {
            double hostSlowness = getHostSlowness(hostName, currentTime);
            double hostSpeed = maxPossibleSlowness - hostSlowness;
            currentSlowness.put(hostName, hostSlowness);

            // Find the slow hosts
            if (hostSlowness > maxNormalSlowness) {
                slowHosts.put(hostName, hostSpeed);
            }

            if (hostSlowness > maxSlowness) {
                maxSlowness = hostSlowness;
                slowestHost = hostName;
            }
        }

        if (slowHosts.size() == 0) {
            // This is the case were all hosts are good. We just want to keep our choices within the fastest
            // ones, so we remove the slowest one before we choose

            hostNames.remove(slowestHost);
            currentSlowness.remove(slowestHost);

            // Choose a host probabilistically
            return pickAHostUsingCollectedStatistics(hostNames, currentSlowness, queriesWeight, true);
        } else {
            // Now we have more than one slow host.
            if (slowHosts.size() == currentSlowness.size()) {
                // And they happen to be all the hosts we got. Return the one with the least queries
                return pickAHostUsingCollectedStatistics(hostNames, currentSlowness, 1, false);
            } else {
                // Remove all the slow ones, which can be more than one but cannot be all
                for (Map.Entry<String, Double> e : slowHosts.entrySet()) {
                    currentSlowness.remove(e.getKey());
                }

                // Pick probabilistically
                return pickAHostUsingCollectedStatistics(hostNames, currentSlowness, queriesWeight, true);
            }
        }
    }

    static Thread LoggerThread = null;
    static boolean stopLogging = false;

    /**
     * Starts the host slowness logger with the default values
     */
    public void logHostSlownessData() {
        logHostSlownessData(1000);
    }

    /**
     * Runs a thread that keeps on drawing host slowness chart every updatePeriod.
     *
     * @param updatePeriod The number of milliseconds between updates. If null, the default is 1000
     */
    public void logHostSlownessData(final int updatePeriod) {
        // Check if we already started the thread
        if (LoggerThread != null) {
            return;
        }

        // Check if we were able to allocate the rolling log
        if (slowHostLog == null) {
            log.warn("ERROR: Cannot log host slowness because the rolling log is null");
            return;
        }

        LoggerThread = new Thread() {
            @Override
            public void run() {
                slowHostLog.writeLine("#Logger starting");
                while (!stopLogging) {
                    try {
                        sleep(updatePeriod);
                    } catch (Exception e) {
                        log.warn("Interrupted while writing the log...");
                        break;
                    }

                    // Is there any data to draw ??
                    if (hostSlowScore.size() == 0) {
                        // We'll keep waiting, until some data comes in....
                        continue;
                    }

                    String[] hostNames = new String[hostSlowScore.size()];
                    hostNames = hostSlowScore.keySet().toArray(hostNames);

                    // Store a sample
                    long currentTime = System.currentTimeMillis();
                    StringBuilder logLine = new StringBuilder();
                    logLine.append("SampleType:SlownessSample");
                    logLine
                            .append(",t=")
                            .append(currentTime);
                    for (String hostName : hostNames) {
                        logLine
                                .append(",")
                                .append(hostName)
                                .append("=")
                                .append(getHostSlowness(hostName, currentTime));
                    }

                    slowHostLog.writeLine(logLine.toString());
                }

                try {
                    slowHostLog.close();
                } catch (IOException e) {
                    log.warn("Error closing the slowness log");
                }

                stopLogging = false;
                LoggerThread = null;
            }
        };
        LoggerThread.start();
    }

    public String getJsonMetrics() {
        StringBuilder metrics = new StringBuilder();
        HostMetadataTracker metadataTracker = hostMetric.getMetadataTracker();
        HashMap<String, Integer> queriesPerHost = metadataTracker.getQueriesPerHostList();
        metrics.append("{");
        metrics.append("\"queryhistogram\":{");
        boolean addComma = false;
        for (Map.Entry<String, Integer> e : queriesPerHost.entrySet()) {
            if (addComma) metrics.append(",\n");
            metrics.append("\"")
                    .append(e.getKey().replace("\n", ""))
                    .append("\": ")
                    .append(e.getValue());
            addComma = true;
        }
        metrics.append("},");
        metrics.append("\"hostslowness\":{");
        String[] hostNames = new String[hostSlowScore.size()];
        hostNames = hostSlowScore.keySet().toArray(hostNames);
        long currentTime = System.currentTimeMillis();
        addComma = false;
        for (String hostName : hostNames) {
            if (addComma) metrics.append(",\n");
            metrics
                    .append("\"")
                    .append(hostName.replace("\n", ""))
                    .append("\": ")
                    .append(getHostSlowness(hostName, currentTime));
            addComma = true;
        }
        metrics.append("}");
        metrics.append("}");
        return metrics.toString();
    }
}
