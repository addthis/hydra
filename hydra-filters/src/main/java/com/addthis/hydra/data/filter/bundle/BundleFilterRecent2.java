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
package com.addthis.hydra.data.filter.bundle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

import com.addthis.basis.collect.HotMap;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">allows filtering on keys occurring within a sliding time window</span>.
 * <p/>
 * <p>Controls are: key count (number of keys to track) window size (time) min points (items
 * in window before it's activated) max occurrence (cap on occurrence of item
 * inside window).</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 * </pre>
 *
 * @user-reference
 * @hydra-name recent2
 */
public final class BundleFilterRecent2 extends BundleFilter {

    @FieldConfig(codable = true, required = true)
    private AutoField       time;
    @FieldConfig(codable = true, required = true)
    private AutoField       field;
    @FieldConfig(codable = true, required = true)
    private int             keys; // number of unique entries to track
    @FieldConfig(codable = true, required = true)
    private long            timeWindow; // max time window over which events are measured
    @FieldConfig(codable = true, required = true)
    private int             minPoints; // number of data points required for each entry
    @FieldConfig(codable = true)
    private long            minAvgTime; // true under average minTime
    @FieldConfig(codable = true)
    private long            maxOccurrence; // true if count achieved in time window
    @FieldConfig(codable = true)
    private boolean         defaultExit;
    @FieldConfig(codable = true)
    private HashSet<String> exclude;

    @SuppressWarnings("unchecked")
    private HotMap<String, Mark> cache = new HotMap<>(new HashMap());

    @Override
    public void open() { }

    @Override
    public boolean filter(Bundle bundle) {
        ValueLong timeValue = time.getValue(bundle).asLong();
        return timeValue != null ? accept(timeValue.getLong(), field.getValue(bundle)) : false;
    }

    /**
     * @param time  time packet received
     * @param value field value
     * @return true to process, false to abort
     */
    public synchronized boolean accept(long time, ValueObject value) {
        if (exclude != null && exclude.contains(value.asString().toString())) {
            return defaultExit;
        }
        String sv = ValueUtil.asNativeString(value);
        Mark v = cache.get(sv);
        if (v == null) {
            v = new Mark();
            if (cache.put(sv, v) != null) {
                System.out.println("ERROR : cache put hit on " + value);
            }
            if (cache.size() > keys) {
                cache.removeEldest();
            }
        }
        long avtime = v.averageTime(time);
        if (avtime == 0) {
            return defaultExit;
        }
        return (minAvgTime > 0 && avtime <= minAvgTime) ||
               (maxOccurrence > 0 && v.times.size() >= maxOccurrence);
    }

    /** */
    private class Mark {

        public TreeMap<Long, Long> times = new TreeMap<>();

        long averageTime(long time) {
            times.put(time, time);
            long delta = 0;
            while (true) {
                long oldest = times.firstKey();
                long newest = times.lastKey();
                delta = newest - oldest;
                if (delta > timeWindow) {
                    times.remove(oldest);
                    continue;
                }
                break;
            }
            int numPoints = times.size();
            return (numPoints >= minPoints ? delta / numPoints : 0);
        }
    }
}
