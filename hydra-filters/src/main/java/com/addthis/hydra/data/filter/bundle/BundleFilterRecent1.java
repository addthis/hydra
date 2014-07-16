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
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">allows filtering on keys occurring within a time interval a given number of
 * times</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 * </pre>
 *
 * @user-reference
 * @hydra-name recent1
 */
public final class BundleFilterRecent1 extends BundleFilter {

    @FieldConfig(codable = true, required = true)
    private String          time;
    @FieldConfig(codable = true, required = true)
    private String          field;
    @FieldConfig(codable = true)
    private int             track; // number of unique entries to track
    @FieldConfig(codable = true)
    private int             sample; // number of data points to sample for each entry
    @FieldConfig(codable = true)
    private long            minTime; // true under minTime
    @FieldConfig(codable = true)
    private long            maxTime; // true over maxTime
    @FieldConfig(codable = true)
    private long            maxCount; // absolute max count for a key
    @FieldConfig(codable = true)
    private boolean         defaultExit;
    @FieldConfig(codable = true)
    private HashSet<String> exclude;

    @SuppressWarnings("unchecked")
    private HotMap<String, Mark> cache = new HotMap<>(new HashMap());
    private String[] fields;

    @Override
    public void initialize() {
        fields = new String[]{time, field};
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        BundleField[] bound = getBindings(bundle, fields);
        ValueLong time = bundle.getValue(bound[0]).asLong();
        return time != null ? accept(time.getLong(), bundle.getValue(bound[1])) : false;
    }

    /**
     * @param time  time packet received
     * @param value field value
     * @return true to process, false to abort
     */
    public synchronized boolean accept(long time, ValueObject value) {
        if (exclude != null && exclude.contains(value)) {
            return defaultExit;
        }
        String sv = ValueUtil.asNativeString(value);
        Mark v = cache.get(sv);
        if (v == null) {
            v = new Mark();
            if (cache.put(sv, v) != null) {
                System.out.println("ERROR : cache put hit on " + value);
            }
            if (cache.size() > track) {
                cache.removeEldest();
            }
        }
        long avtime = v.averageTime(time);
        return avtime == 0 ?
               defaultExit :
               (maxCount > 0 && v.count > maxCount) ||
               ((minTime == 0 || avtime < minTime) && (avtime > maxTime));
    }

    /** */
    private class Mark {

        public int count;
        public long cumTime;
        public TreeMap<Long, Long> times = new TreeMap<>();

        long averageTime(long time) {
            count++;
            times.put(time, time);
            if (times.size() > sample) {
                times.remove(times.firstKey());
            }
            long delta = times.lastKey() - times.firstKey();
            return (count >= sample ? delta / sample : 0);
        }
    }
}
