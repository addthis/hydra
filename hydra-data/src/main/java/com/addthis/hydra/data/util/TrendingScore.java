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
package com.addthis.hydra.data.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.addthis.codec.Codec;

/**
 * A trending algorithm that recognises rising edge based on cumulative percentage change in hits
 */
public class TrendingScore implements Codec.Codable {

    // 15 mins
    private static final long PERIOD_BETWEEN_RECALCULATION = 900000;
    private double alpha = 0.3;
    private int minResults = 50;
    private static final String DEFAULT_URL_SEP = "/";

    public TrendingScore() {

    }

    public TrendingScore(double alpha, int minResults) {
        this.minResults = minResults;
        this.alpha = alpha;
    }

    public List<URLTree.TreeObject.TreeValue> getTrends(TreeMap<String, KeyTopper> hourlyTrends, TreeMap<String, KeyTopper> dailyTrends, TreeMap<String, KeyTopper> monthlyTrends) {
        List<URLTree.TreeObject.TreeValue> sortedScore = null;

        URLTree scores = new URLTree();

        if (calculateTrends(scores, hourlyTrends, 5) ||
            calculateTrends(scores, dailyTrends, 3) ||
            calculateTrends(scores, monthlyTrends, 2) ||
            scores.size() > 0) {
            sortedScore = sortByValue(scores);
        }

        return sortedScore;
    }

    private boolean calculateTrends(URLTree scores, TreeMap<String, KeyTopper> trends, long timeNormalizingFactor) {
        boolean success = false;

        if (trends != null && trends.size() > 0) {
            int results = 0;
            // create a list of unique urls
            Set<String> urls = new HashSet<String>();
            // create a map of time --> (url, hits)
            Map<String, Map<String, Long>> trendingMap = buildTrendingMap(trends, urls);

            for (String url : urls) {
                double score = 0;

                // these time stamps will be in order (TreeMap impl)
                for (String timeStamp : trendingMap.keySet()) {
                    Long count = trendingMap.get(timeStamp).get(url);

                    if (count != null) {
                        score = ema(score, count);
                    }
                }

                // List<String> urlPath = branched(url);
                // remove protocol
                url = url.replaceAll("^http://", "");

                scores.addURLPath(url, score * timeNormalizingFactor);
                results++;
            }

            if (results >= minResults) {
                success = true;
            }
        }

        return success;
    }

    private List<String> branched(String url) {
        List<String> urlPath = null;

        try {
            urlPath = Arrays.asList(url.replaceAll("^http://", "").split(DEFAULT_URL_SEP, -1));
        } catch (Exception e) {
        }

        return urlPath;
    }

    public double ema(double prevValue, double currValue) {
        return prevValue > 0 ? prevValue + alpha * (currValue - prevValue) : currValue;
    }

    private double percentageChangeBetween(long prevCount, long currentCount) {
        if (prevCount == 0 || currentCount == 0) {
            return 0;
        } else {
            return Math.abs(100 - ((currentCount * 100) / prevCount));
        }
    }

    private Map<String, Map<String, Long>> buildTrendingMap(Map<String, KeyTopper> timeSeriesMap, Set<String> urls) {
        Map trendingMap = new TreeMap<String, Map<String, Long>>();

        for (String ts : timeSeriesMap.keySet()) {
            Map<String, Long> urlCount = new HashMap<String, Long>();
            trendingMap.put(ts, urlCount);

            for (Map.Entry<String, Long> url : timeSeriesMap.get(ts).getSortedEntries()) {
                urls.add(url.getKey());
                urlCount.put(url.getKey(), url.getValue());
            }
        }

        return trendingMap;
    }

    public static List<URLTree.TreeObject.TreeValue> sortByValue(URLTree tree) {
        List<URLTree.TreeObject.TreeValue> list = new LinkedList<URLTree.TreeObject.TreeValue>(tree.getBranches(DEFAULT_URL_SEP));

        Collections.sort(list, new Comparator<URLTree.TreeObject.TreeValue>() {
            @Override
            public int compare(URLTree.TreeObject.TreeValue t1, URLTree.TreeObject.TreeValue t2) {
                return -t1.getValue().compareTo(t2.getValue());
            }
        });

        return list;
    }
}
