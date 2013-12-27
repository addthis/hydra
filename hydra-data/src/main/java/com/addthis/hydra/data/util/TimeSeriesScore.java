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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import com.addthis.codec.Codec;

/**
 * Class that will assign a score to a Url based on its trend over time
 */
public class TimeSeriesScore implements Codec.Codable {

    double alpha = 0.3;

    public TimeSeriesScore() {
    }

    /**
     * returns the list of urls sorted by trending score from largest to smallest count.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Map.Entry<String, Long>[] getSortedEntries(Map<String, KeyTopper> timeSeriesMap) {
        //timeSeriesMap = getSortedTimeSeriesMap(timeSeriesMap);

        Object[] dhArr = timeSeriesMap.keySet().toArray();
        String firstStr = (String) dhArr[0];
        String lastStr = (String) dhArr[dhArr.length - 1];

        String pattern = "yyMMddHH";
        SimpleDateFormat format = new SimpleDateFormat(pattern);

        Date firstTS = format.parse(firstStr, new ParsePosition(0));
        Date lastTS = format.parse(lastStr, new ParsePosition(0));
        int totalBins = (int) ((lastTS.getTime() - firstTS.getTime()) / 3600000) + 1;

        Map<String, Integer> timeBinLookup = new HashMap<String, Integer>();
        timeBinLookup.put(firstStr, 1);
        timeBinLookup.put(lastStr, totalBins);

        Date currenthour = firstTS;
        Calendar c = Calendar.getInstance();
        for (int i = 2; i < totalBins; i++) {
            c.setTime(currenthour);
            c.add(Calendar.HOUR_OF_DAY, 1);
            currenthour = c.getTime();
            timeBinLookup.put(format.format(currenthour), i);
        }

        List<KeyTopper> arrKT = new ArrayList<KeyTopper>();
        Map.Entry<String, Long>[] KTmap;
        Set<String> urls = new HashSet<String>();

        Iterator<Entry<String, KeyTopper>> it = timeSeriesMap.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, KeyTopper> pairs = it.next();
            arrKT.add(pairs.getValue());
            KTmap = pairs.getValue().getSortedEntries();
            for (Entry<String, Long> entry : KTmap) {
                urls.add(entry.getKey());
            }
        }


        Map<String, Long> map = new HashMap<String, Long>();
        Iterator<String> itr = urls.iterator();
        while (itr.hasNext()) {
            String urlItr = itr.next();
            if (urlItr != null) {
                double meanSoFar = 0.0;
                double normalizedCount;
                double trendingScore = 0.0;
                int lastUpdated = 0;

                for (int i = 0; i < arrKT.size(); i++) {

                    int binNo = timeBinLookup.get((String) dhArr[i]);
                    Long count = arrKT.get(i).get(urlItr);
                    Long currCount = 0L;

                    while (lastUpdated < binNo) {
                        lastUpdated = lastUpdated + 1;
                        if (lastUpdated != binNo) {
                            currCount = 0L;
                        } else {
                            currCount = (count == null) ? 0L : count;
                        }
                        meanSoFar = UpdateMeanSoFar(lastUpdated, currCount, meanSoFar);
                        normalizedCount = currCount - meanSoFar;
                        trendingScore = alpha * normalizedCount + (1 - alpha) * trendingScore;
                    }
                }
                map.put(urlItr, (long) (1000 * trendingScore));
            }
        }

        Map.Entry e[] = new Map.Entry[map.size()];

        e = map.entrySet().toArray(e);
        Arrays.sort(e, new Comparator() {
            public int compare(Object arg0, Object arg1) {
                if (((Long) ((Map.Entry) arg1).getValue()) > ((Long) ((Map.Entry) arg0).getValue())) {
                    return 1;
                } else if (((Long) ((Map.Entry) arg1).getValue()) < ((Long) ((Map.Entry) arg0).getValue())) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });

        return e;

    }

    /**
     * Update the Mean
     */
    public double UpdateMeanSoFar(int BinNo, Long BinCount, double MeanSoFar) {
        return ((MeanSoFar * (BinNo - 1) + BinCount) / BinNo);
    }

    /**
     * Get SortedTimeSeriesMap sorted by the key
     *
     * @param timeSeriesMap
     * @return
     */
    public Map<String, KeyTopper> getSortedTimeSeriesMap(Map<String, KeyTopper> timeSeriesMap) {

        Map<String, KeyTopper> sortedTimeSeriesMap = new LinkedHashMap<String, KeyTopper>();
        TreeSet<String> keys = new TreeSet<String>(timeSeriesMap.keySet());
        for (String key : keys) {
            KeyTopper value = timeSeriesMap.get(key);
            if (value != null) {
                sortedTimeSeriesMap.put(key, value);
            }
        }
        return sortedTimeSeriesMap;
    }

}
