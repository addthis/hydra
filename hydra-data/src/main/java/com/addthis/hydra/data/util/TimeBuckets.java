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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import java.text.SimpleDateFormat;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;


/**
 *         A class to store time values in buckets of variable size.
 */
public class TimeBuckets implements Codable {

    /**
     * @param args
     */
    @FieldConfig(codable = true, required = true)
    private TreeMap<Long, Long> map;
    @FieldConfig(codable = true)
    private long blockSize;

    public TimeBuckets() {
    }

    public TimeBuckets init(long size) {
        blockSize = size;
        map = new TreeMap<Long, Long>();
        return this;
    }

    public void initBuckets(String date) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmm");
            try {
                Date jobDate = sdf.parse(date + "2359");
                sdf.setTimeZone(TimeZone.getTimeZone("US/Eastern"));
                Long l = jobDate.getTime();
                this.acceptEmpty(l);
            } catch (Exception e) {
            }

        }
    }

    public void accept(Long time) {
        Long keyval = toKey(time);
        Long count = map.get(keyval);
        if (count == null) {
            if (map.size() > 0) {
                // Add empty buckets as necessary
                if (map.firstKey() - keyval > blockSize) {
                    Long firstKey = map.firstKey();
                    for (Long l = keyval + blockSize; l < firstKey; l += blockSize) {
                        map.put(l, 0L);
                    }
                }
                if (keyval - map.lastKey() > blockSize) {
                    for (Long l = map.lastKey() + blockSize; l < keyval; l += blockSize) {
                        map.put(l, 0L);
                    }
                }
            }
            map.put(keyval, 1L);
        } else {
            map.put(keyval, count + 1);
        }
    }

    public void acceptEmpty(Long time) {
        Long keyval = toKey(time);
        map.put(keyval, 0L);
    }

    public TreeMap<Long, Long> getMap() {
        return map;
    }

    public Long[] getCounts() {
        Long[] rv = new Long[1];
        rv = map.values().toArray(rv);
        return rv;
    }

    public int size() {
        return map.size();
    }

    @SuppressWarnings("unchecked")
    public Map.Entry<String, Long>[] getEntries() {
        Map.Entry[] e = new Map.Entry[map.size()];
        e = map.entrySet().toArray(e);
        return e;
    }

    public TreeMap<String, Long> getChangeTimes(double minRatio, int minSize, double minZScore, int inactiveThreshold, int windowSize) {
        TreeMap<String, Long> rv = new TreeMap<String, Long>();
        Long[] counts = this.getCounts();
        double mean = FindChangePoints.mean(counts);
        if (mean > 10) {
            List<ChangePoint> cps = FindChangePoints.findSignificantPoints(counts, minSize, minRatio, minZScore, inactiveThreshold, windowSize);
            for (ChangePoint cp : cps) {
                Integer index = cp.getIndex();
                String key = cp.getType().name() + "," + map.keySet().toArray()[index];
                rv.put(key, cp.getSize());
            }
        }
        return rv;
    }

    public Map<Long, Long> getPeaks(int maxWidth, int minHt) {
        Long[] counts = this.getCounts();
        List<ChangePoint> peaks = FindChangePoints.findHighPoints(counts, maxWidth, minHt);
        Map<Long, Long> rv = new HashMap<Long, Long>();
        for (ChangePoint peak : peaks) {
            int index = peak.getIndex();
            Long peakTime = (Long) map.keySet().toArray()[index];
            rv.put(peakTime, peak.getSize());
        }
        return rv;
    }

    private Long toKey(Long time) {
        return (time / blockSize) * blockSize;
    }
}
