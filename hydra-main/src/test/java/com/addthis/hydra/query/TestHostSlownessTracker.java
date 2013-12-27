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

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

public class TestHostSlownessTracker {

    String chartTemplate = "<!--\n" +
                           "You are free to copy and use this sample in accordance with the terms of the\n" +
                           "Apache license (http://www.apache.org/licenses/LICENSE-2.0.html)\n" +
                           "-->\n" +
                           "\n" +
                           "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">\n" +
                           "<html xmlns=\"http://www.w3.org/1999/xhtml\">\n" +
                           "  <head>\n" +
                           "    <meta http-equiv=\"content-type\" content=\"text/html; charset=utf-8\"/>\n" +
                           "    <title>\n" +
                           "      Google Visualization API Sample\n" +
                           "    </title>\n" +
                           "    <script type=\"text/javascript\" src=\"http://www.google.com/jsapi\"></script>\n" +
                           "    <script type=\"text/javascript\">\n" +
                           "      google.load('visualization', '1', {packages: ['corechart']});\n" +
                           "    </script>\n" +
                           "    <script type=\"text/javascript\">\n" +
                           "      function drawVisualization() {\n" +
                           "        // Create and populate the data table.\n" +
                           "        var data = google.visualization.arrayToDataTable([\n" +
                           "          {{$ChartTable$}}\n" +        // Replace {{$ChartTable$}} with the chart table.
                           "        ]);\n" +
                           "      \n" +
                           "        // Create and draw the visualization.\n" +
                           "        new google.visualization.LineChart(document.getElementById('visualization')).\n" +
                           "            draw(data, {curveType: \"line\",\n" +
                           "                        vAxis: {maxValue: {{$maxValue$}}}}\n" + // replace {{$maxValue}} with the max value
                           "                );\n" +
                           "      }\n" +
                           "      \n" +
                           "\n" +
                           "      google.setOnLoadCallback(drawVisualization);\n" +
                           "    </script>\n" +
                           "  </head>\n" +
                           "  <body style=\"font-family: Arial;border: 0 none;\">\n" +
                           "    <div id=\"visualization\" style=\"width: 1024px; height: 768px;\"></div>\n" +
                           "  </body>\n" +
                           "</html>\n" +
                           "​";

    public String hostName(int hostNumber) {
        return String.format("h%02d", hostNumber);
    }

    //@Test
    public void TurnAroundTimeTest3RandomSlowHosts() throws Exception {
        final HostMetadataTracker metaDataTracker = new HostMetadataTracker();
        HostMetricTurnaroundTime turnaroundTime = new HostMetricTurnaroundTime(metaDataTracker, 2);
        final HostSlownessTracker slownessTracker = new HostSlownessTracker(turnaroundTime);

        // Set the time constant to 12 seconds
        slownessTracker.setDecayTimeConstant(12000);

        // Now run 100 threads simulating 100 queries
        // running over 10 hosts. Every query needs 4 hosts to run. Every query will run 10 times
        final AtomicInteger runningQueries = new AtomicInteger(0);

        final ConcurrentHashMap<String, Boolean> slowHosts = new ConcurrentHashMap<String, Boolean>();
        slowHosts.putIfAbsent("h01", true);
        slowHosts.putIfAbsent("h05", true);
        slowHosts.putIfAbsent("h07", true);

        for (int i = 0; i != 10; i++) {
            // Every thread is a user running a query
            new Thread() {
                @Override
                public void run() {
                    try {
                        int myNumber = runningQueries.incrementAndGet();
                        String queryID = "" + myNumber;
                        System.out.println("Running: " + myNumber);
                        Random rGen = new Random(System.currentTimeMillis() + myNumber);

                        // The user runs the query sample times (here is 10)
                        for (int samples = 0; samples != 50; samples++) {
                            // There's a 10% chance that the slow hosts will change
                            if (rGen.nextInt(100) < 10) {
                                slowHosts.remove(slowHosts.keySet().iterator().next());
                                while (slowHosts.size() < 3) {
                                    slowHosts.putIfAbsent(hostName(rGen.nextInt(10)), true);
                                }
                            }

                            sleep(rGen.nextInt(1000));
                            // Pick four hosts for this query
                            Vector<String> queryFastHosts = new Vector<String>();
                            HashMap<String, Long> hostStartTimes = new HashMap<String, Long>();

                            // Every query is composed of 4 parts
                            for (int j = 0; j != 4; j++) {
                                // This piece of the query can be executed on any of those three machines
                                HashSet<String> possibleHosts = new HashSet<String>();
                                boolean gotASlowHost = false;
                                for (int k = 0; k != 3; k++) {
                                    String possibleHost;
                                    boolean repeat;
                                    do {
                                        possibleHost = hostName(rGen.nextInt(10));

                                        repeat = (gotASlowHost && slowHosts.containsKey(possibleHost));

                                        if (!gotASlowHost && slowHosts.containsKey(possibleHost)) {
                                            gotASlowHost = true;
                                        }
                                    } while (possibleHosts.contains(possibleHost) || repeat);
                                    possibleHosts.add(possibleHost);
                                }

                                String host = slownessTracker.pickAHost(possibleHosts);
                                if (!slowHosts.containsKey(host)) {
                                    queryFastHosts.add(host);
                                }

                                // Set the start time
                                long startTime = System.currentTimeMillis();
                                hostStartTimes.put(host, startTime);
                                metaDataTracker.addHostToQuery(queryID, host);
                            }

                            sleep(rGen.nextInt(100));
                            // Set the end time of the fast hosts
                            for (int i = 0; i != queryFastHosts.size(); i++) {
                                sleep(rGen.nextInt(50));
                                String host = queryFastHosts.get(i);
                                assert (queryID != null);
                                assert (host != null);
                                assert (hostStartTimes.get(host) != null);
                                metaDataTracker.addHostDeltaTime(queryID, host,
                                        System.currentTimeMillis() - hostStartTimes.get(host));
                            }

                            // Now deal with slow hosts
                            if (slowHosts.size() > 0) {
                                // Since we were hit by one slow host, then one more straggler
                                HashSet<String> possibleHosts = new HashSet<String>();
                                boolean gotASlowHost = false;
                                for (int k = 0; k != 2; k++) {
                                    String possibleHost;
                                    do {
                                        possibleHost = hostName(rGen.nextInt(10));
                                    }
                                    while (possibleHosts.contains(possibleHost) || (gotASlowHost && slowHosts.containsKey(possibleHost)));

                                    if (!gotASlowHost && slowHosts.containsKey(possibleHost)) {
                                        gotASlowHost = true;
                                    }
                                    possibleHosts.add(possibleHost);
                                }

                                String host = slownessTracker.pickAHost(possibleHosts);
                                if (!slowHosts.containsKey(host)) {
                                    queryFastHosts.add(host);
                                }

                                metaDataTracker.setHostAsRusher(queryID, host);
                                long startTime = System.currentTimeMillis();
                                hostStartTimes.put(host, startTime);
                                metaDataTracker.addHostToQuery(queryID, host);
                            }

                            sleep(rGen.nextInt(100));

                            // Now set the final data and read the speeds
                            // Set the end time of the fast hosts
                            for (int i = 0; i != queryFastHosts.size(); i++) {
                                sleep(rGen.nextInt(50));
                                String host = queryFastHosts.get(i);
                                metaDataTracker.addHostDeltaTime(queryID, host,
                                        System.currentTimeMillis() - hostStartTimes.get(host));
                            }

                            slownessTracker.commitQueryHostMetrics(queryID);
                            metaDataTracker.deleteQueryMetrics(queryID);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    } finally {
                        runningQueries.decrementAndGet();
                    }
                }
            }.start();
        }

        // Wait till the threads start
        Thread.sleep(100);

        StringBuilder chartData = new StringBuilder();
        chartData.append("['x'");
        for (int i = 0; i != 10; i++) {
            chartData
                    .append(",'")
                    .append(hostName(i))
                    .append("'");
        }

        chartData.append("]");

        long startTime = System.currentTimeMillis();

        double maxSlowness = 0;
        while (runningQueries.get() != 0) {
            Thread.sleep(100);
            chartData.append(",\n");
            long currentTime = System.currentTimeMillis();
            long time = currentTime - startTime;
            chartData.append("['")
                    .append(time)
                    .append("'");
            for (int i = 0; i != 10; i++) {
                double hostSlowness = slownessTracker.getHostSlowness(hostName(i), currentTime);
                if (hostSlowness > maxSlowness) {
                    maxSlowness = hostSlowness;
                }
                chartData
                        .append(",")
                        .append(hostSlowness)
                        .append("");
            }
            chartData.append("]");
        }

        chartTemplate = chartTemplate.replace("{{$ChartTable$}}", chartData);
        chartTemplate = chartTemplate.replace("{{$maxValue$}}", "" + maxSlowness);

        // Write to output
        String chartFileName = "/tmp/chart3RandomSlow.html";
        Writer out = new OutputStreamWriter(new FileOutputStream(chartFileName));
        try {
            out.write(chartTemplate);
        } finally {
            out.close();
        }
    }

    //@Test
    public void TurnAroundTimeTest3ConsistentSlowHosts() throws Exception {
        final HostMetadataTracker metaDataTracker = new HostMetadataTracker();
        HostMetricTurnaroundTime turnaroundTime = new HostMetricTurnaroundTime(metaDataTracker, 2);
        final HostSlownessTracker slownessTracker = new HostSlownessTracker(turnaroundTime);

        // Set the time constant to 12 seconds
        slownessTracker.setDecayTimeConstant(12000);

        // Now run 100 threads simulating 100 queries
        // running over 10 hosts. Every query needs 4 hosts to run. Every query will run 10 times
        final AtomicInteger runningQueries = new AtomicInteger(0);

        final Vector<String> slowHosts = new Vector<String>();
        slowHosts.addAll(
                Arrays.asList("h01", "h05", "h07")
        );

        for (int i = 0; i != 10; i++) {
            // Every thread is a user running a query
            new Thread() {
                @Override
                public void run() {
                    try {
                        int myNumber = runningQueries.incrementAndGet();
                        String queryID = "" + myNumber;
                        System.out.println("Running: " + myNumber);
                        Random rGen = new Random(System.currentTimeMillis() + myNumber);

                        // The user runs the query sample times (here is 10)
                        for (int samples = 0; samples != 50; samples++) {
                            sleep(rGen.nextInt(1000));
                            // Pick four hosts for this query
                            Vector<String> queryFastHosts = new Vector<String>();
                            HashMap<String, Long> hostStartTimes = new HashMap<String, Long>();

                            // Every query is composed of 4 parts
                            for (int j = 0; j != 4; j++) {
                                // This piece of the query can be executed on any of those three machines
                                HashSet<String> possibleHosts = new HashSet<String>();
                                boolean gotASlowHost = false;
                                for (int k = 0; k != 3; k++) {
                                    String possibleHost;
                                    boolean repeat;
                                    do {
                                        possibleHost = hostName(rGen.nextInt(10));

                                        repeat = (gotASlowHost && slowHosts.contains(possibleHost));

                                        if (!gotASlowHost && slowHosts.contains(possibleHost)) {
                                            gotASlowHost = true;
                                        }
                                    } while (possibleHosts.contains(possibleHost) || repeat);
                                    possibleHosts.add(possibleHost);
                                }

                                String host = slownessTracker.pickAHost(possibleHosts);
                                if (!slowHosts.contains(host)) {
                                    queryFastHosts.add(host);
                                }

                                // Set the start time
                                long startTime = System.currentTimeMillis();
                                hostStartTimes.put(host, startTime);
                                metaDataTracker.addHostToQuery(queryID, host);
                            }

                            sleep(rGen.nextInt(100));
                            // Set the end time of the fast hosts
                            for (int i = 0; i != queryFastHosts.size(); i++) {
                                sleep(rGen.nextInt(50));
                                String host = queryFastHosts.get(i);
                                assert (queryID != null);
                                assert (host != null);
                                assert (hostStartTimes.get(host) != null);
                                metaDataTracker.addHostDeltaTime(queryID, host,
                                        System.currentTimeMillis() - hostStartTimes.get(host));
                            }

                            // Now deal with slow hosts
                            if (slowHosts.size() > 0) {
                                // Since we were hit by one slow host, then one more straggler
                                HashSet<String> possibleHosts = new HashSet<String>();
                                boolean gotASlowHost = false;
                                for (int k = 0; k != 2; k++) {
                                    String possibleHost;
                                    do {
                                        possibleHost = hostName(rGen.nextInt(10));
                                    }
                                    while (possibleHosts.contains(possibleHost) || (gotASlowHost && slowHosts.contains(possibleHost)));

                                    if (!gotASlowHost && slowHosts.contains(possibleHost)) {
                                        gotASlowHost = true;
                                    }
                                    possibleHosts.add(possibleHost);
                                }

                                String host = slownessTracker.pickAHost(possibleHosts);
                                if (!slowHosts.contains(host)) {
                                    queryFastHosts.add(host);
                                }

                                metaDataTracker.setHostAsRusher(queryID, host);
                                long startTime = System.currentTimeMillis();
                                hostStartTimes.put(host, startTime);
                                metaDataTracker.addHostToQuery(queryID, host);
                            }

                            sleep(rGen.nextInt(100));

                            // Now set the final data and read the speeds
                            // Set the end time of the fast hosts
                            for (int i = 0; i != queryFastHosts.size(); i++) {
                                sleep(rGen.nextInt(50));
                                String host = queryFastHosts.get(i);
                                metaDataTracker.addHostDeltaTime(queryID, host,
                                        System.currentTimeMillis() - hostStartTimes.get(host));
                            }

                            slownessTracker.commitQueryHostMetrics(queryID);
                            metaDataTracker.deleteQueryMetrics(queryID);
                        }
                    } catch (Exception e) {
                        System.out.println("Exception:");
                        e.printStackTrace();
                        fail(e.getMessage());
                    } finally {
                        runningQueries.decrementAndGet();
                    }
                }
            }.start();
        }

        // Wait till the threads start
        Thread.sleep(100);

        StringBuilder chartData = new StringBuilder();
        chartData.append("['x'");
        for (int i = 0; i != 10; i++) {
            chartData
                    .append(",'")
                    .append(hostName(i))
                    .append("'");
        }

        chartData.append("]");

        long startTime = System.currentTimeMillis();

        double maxSlowness = 0;
        while (runningQueries.get() != 0) {
            Thread.sleep(100);
            chartData.append(",\n");
            long currentTime = System.currentTimeMillis();
            long time = currentTime - startTime;
            chartData.append("['")
                    .append(time)
                    .append("'");
            for (int i = 0; i != 10; i++) {
                double hostSlowness = slownessTracker.getHostSlowness(hostName(i), currentTime);
                if (hostSlowness > maxSlowness) {
                    maxSlowness = hostSlowness;
                }
                chartData
                        .append(",")
                        .append(hostSlowness)
                        .append("");
            }
            chartData.append("]");
        }

        chartTemplate = chartTemplate.replace("{{$ChartTable$}}", chartData);
        chartTemplate = chartTemplate.replace("{{$maxValue$}}", "" + maxSlowness);

        // Write to output
        String chartFileName = "/tmp/chart3ConsistentSlow.html";
        Writer out = new OutputStreamWriter(new FileOutputStream(chartFileName));
        try {
            out.write(chartTemplate);
        } finally {
            out.close();
        }
    }
}
