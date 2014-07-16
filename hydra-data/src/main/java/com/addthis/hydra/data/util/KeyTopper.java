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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;


/**
 * Class that helps maintain a top N list for any String Map TODO should move
 * into basis libraries
 */
public final class KeyTopper implements Codable {

    public KeyTopper() {
    }

    @FieldConfig(codable = true, required = true)
    private HashMap<String, Long> map;
    @FieldConfig(codable = true)
    private long minVal;
    @FieldConfig(codable = true)
    private String minKey;
    @FieldConfig(codable = true)
    private boolean lossy;

    @Override
    public String toString() {
        return "topper(min:" + minKey + "=" + minVal + "->" + map.toString() + ",lossy:" + lossy + ")";
    }

    public KeyTopper init() {
        map = new HashMap<String, Long>();
        return this;
    }

    public KeyTopper setLossy(boolean isLossy) {
        lossy = isLossy;
        return this;
    }

    public boolean isLossy() {
        return lossy;
    }

    public int size() {
        return map.size();
    }

    public Long get(String key) {
        return map.get(key);
    }

    /**
     * returns the list sorted by greatest to least count.
     */
    @SuppressWarnings("unchecked")
    public Map.Entry<String, Long>[] getSortedEntries() {
        Map.Entry[] e = new Map.Entry[map.size()];
        e = map.entrySet().toArray(e);
        Arrays.sort(e, new Comparator() {
            public int compare(Object arg0, Object arg1) {
                return (int) (((Long) ((Map.Entry) arg1).getValue()) - ((Long) ((Map.Entry) arg0).getValue()));
            }
        });
        return e;
    }

    /** */
    private void recalcMin(boolean maxed, boolean newentry, String id) {
        if (minKey == null || (maxed && newentry) || (!newentry && id.equals(minKey))) {
            minVal = 0;
            for (Map.Entry<String, Long> e : this.map.entrySet()) {
                if (minVal == 0 || e.getValue() < minVal) {
                    minVal = e.getValue();
                    minKey = e.getKey();
                }
            }
        }
    }

    /**
     * Adds 'ID' the top N if: 1) there are more empty slots or 2) count >
     * smallest top count in the list
     *
     * @param id
     * @return element dropped from top or null if accepted into top with no
     *         drops
     */
    public String increment(String id, int maxsize) {
        Long count = map.get(id);
        if (count == null) {
            count = Math.max(lossy && (map.size() >= maxsize) ? minVal - 1 : 0L, 0L);
        }
        return update(id, count + 1, maxsize);
    }

    /**
     * Adds 'ID' the top N if: 1) there are more empty slots or 2) count >
     * smallest top count in the list
     * This one increments weight
     *
     * @param id
     * @param weight
     * @return element dropped from top or null if accepted into top with no
     *         drops
     */
    public String increment(String id, int weight, int maxsize) {
        Long count = map.get(id);

        if (count == null) {
            count = Math.max(lossy && (map.size() >= maxsize) ? minVal - 1 : 0L, 0L);
        }

        return update(id, count + weight, maxsize);
    }

    public String decrement(String id, int maxsize) {
        Long count = map.get(id);
        if (count == null) {
            count = Math.max(lossy && (map.size() >= maxsize) ? minVal + 1 : 0L, 0L);
        }
        return update(id, count - 1, maxsize);
    }

    /**
     * Increments the count for 'ID' in the top map if 'ID' already exists in
     * the map. This method is used if you want to increment a lossy top without
     * removing an element. Used when there is a two stage update for new data
     * elements
     *
     * @param id the id to increment if it already exists in the map
     * @return whether the element was in the map
     */
    public boolean incrementExisting(String id) {
        if (map.containsKey(id)) {
            map.put(id, get(id) + 1);
            return true;
        }
        return false;
    }

    /**
     * Adds 'ID' the top N if: 1) there are more empty slots or 2) count >
     * smallest top count in the list
     *
     * @param id
     * @param count
     * @return element dropped from top or null if accepted into top with no
     *         drops. returns the offered key if it was rejected for update
     *         or inclusion in the top.
     */
    public String update(String id, long count, int maxsize) {
        String removed = null;
        /** should go into top */
        if (count >= minVal) {
            boolean newentry = map.get(id) == null;
            boolean maxed = map.size() >= maxsize;
            // only remove if topN is full and we're not updating an existing entry
            if (maxed && newentry) {
                if (minKey == null && minVal == 0) {
                    recalcMin(maxed, newentry, id);
                }
                map.remove(minKey);
                removed = minKey;
            }
            // update or add entry
            map.put(id, count);
            // recalc min *only* if the min entry was removed or updated
            // checking for null minkey is critical check for empty topN as it
            // sets first min
            if (count == minVal) {
                minKey = id;
            } else {
                recalcMin(maxed, newentry, id);
            }
        }
        /** should go into top */
        else if (map.size() < maxsize) {
            map.put(id, count);
            if (minKey == null || count < minVal) {
                minKey = id;
                minVal = count;
            }
        }
        /** not eligible for top */
        else {
            removed = id;
        }
        return removed;
    }
}
