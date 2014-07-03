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
import java.util.List;
import java.util.Map;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;


/**
 * Class that helps maintain a top N list for any String Map TODO should move
 * into basis libraries
 */
public final class FeaturesKeyTopper implements Codable {

    @FieldConfig(codable = true, required = true)
    private HashMap<String, ReplaceableFeaturesBucket> featuresMap;
    @FieldConfig(codable = true)
    private long minVal;
    @FieldConfig(codable = true)
    private String minKey;
    @FieldConfig(codable = true)
    private boolean lossy;

    public FeaturesKeyTopper() {

    }

    @Override
    public String toString() {
        return "topper(min:" + minKey + "=" + minVal + "->" + featuresMap.toString() + ",lossy:" + lossy + ")";
    }

    public FeaturesKeyTopper init() {
        featuresMap = new HashMap<String, ReplaceableFeaturesBucket>();
        return this;
    }

    public FeaturesKeyTopper setLossy(boolean isLossy) {
        lossy = isLossy;
        return this;
    }

    public boolean isLossy() {
        return lossy;
    }

    public int size() {
        return featuresMap.size();
    }

    public ReplaceableFeaturesBucket get(String key) {
        return featuresMap.get(key);
    }

    /**
     * returns the list sorted by greatest to least count.
     */
    @SuppressWarnings("unchecked")
    public Map.Entry<String, Long>[] getSortedEntries() {
        Map<String, Long> summaryMap = new HashMap<String, Long>();

        for (String id : featuresMap.keySet()) {
            ReplaceableFeaturesBucket bucket = featuresMap.get(id);
            summaryMap.put(id + ">" + bucket.toString(), new Long(bucket.getHits()));
        }

        Map.Entry e[] = new Map.Entry[summaryMap.size()];
        e = summaryMap.entrySet().toArray(e);

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
            for (Map.Entry<String, ReplaceableFeaturesBucket> e : this.featuresMap.entrySet()) {
                if (minVal == 0 || e.getValue().getHits() < minVal) {
                    minVal = e.getValue().getHits();
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
    public String increment(String id, int maxsize, String... features) {
        return increment(id, maxsize, Arrays.asList(features));
    }

    /**
     * Adds 'ID' the top N if: 1) there are more empty slots or 2) count >
     * smallest top count in the list
     *
     * @param id
     * @return element dropped from top or null if accepted into top with no
     *         drops
     */
    public String increment(String id, int maxsize, List<String> features) {
        ReplaceableFeaturesBucket featuresBucket = featuresMap.get(id);

        if (featuresBucket != null) {
            if (features != null) {
                featuresBucket.addFeatures(features);
            }
        } else {
            if (features != null) {
                featuresBucket = new ReplaceableFeaturesBucket();
                featuresBucket.setHits(Math.max(lossy && (featuresMap.size() >= maxsize) ? minVal - 1 : 0l, 0l));
                featuresBucket.addFeatures(features);
            }
        }

        // increment hits regardless of features != null (unique use case)
        featuresBucket.incrementHits(features.size());

        return update(id, maxsize, featuresBucket);
    }

    public void augmentFeatures(String original, List<String> values) {
        if (values != null && values.size() > 0) {
            for (ReplaceableFeaturesBucket featuresBucket : featuresMap.values()) {
                featuresBucket.augmentExistingFeatures(original, values);
            }
        }
    }

    /**
     * Adds 'ID' the top N if: 1) there are more empty slots or 2) count >
     * smallest top count in the list
     *
     * @param id
     * @param featuresBucket
     * @return element dropped from top or null if accepted into top with no
     *         drops. returns the offered key if it was rejected for update
     *         or inclusion in the top.
     */
    public String update(String id, int maxsize, ReplaceableFeaturesBucket featuresBucket) {
        String removed = null;
        /** should go into top */
        if (featuresBucket.getHits() >= minVal) {
            boolean newentry = featuresMap.get(id) == null;
            boolean maxed = featuresMap.size() >= maxsize;
            // only remove if topN is full and we're not updating an existing entry
            if (maxed && newentry) {
                if (minKey == null && minVal == 0) {
                    recalcMin(maxed, newentry, id);
                }
                featuresMap.remove(minKey);
                removed = minKey;
            }
            // update or add entry
            featuresMap.put(id, featuresBucket);
            // recalc min *only* if the min entry was removed or updated
            // checking for null minkey is critical check for empty topN as it
            // sets first min
            if (featuresBucket.getHits() == minVal) {
                minKey = id;
            } else {
                recalcMin(maxed, newentry, id);
            }
        }
        /** should go into top */
        else if (featuresMap.size() < maxsize) {
            featuresMap.put(id, featuresBucket);
            if (minKey == null || featuresBucket.getHits() < minVal) {
                minKey = id;
                minVal = featuresBucket.getHits();
            }
        }
        /** not eligible for top */
        else {
            removed = id;
        }
        return removed;
    }

    public static void main(String args[]) {
        FeaturesKeyTopper keyTopper = new FeaturesKeyTopper();
        keyTopper.init().setLossy(true);
        keyTopper.increment("1", 10000, new String[]{"foo", "bar"});
        keyTopper.increment("1", 10000, new String[]{"bar"});
        keyTopper.increment("2", 10000, new String[]{"one"});
        keyTopper.increment("3", 10000, new String[]{"bar"});
        keyTopper.increment("3", 10000, new String[]{"foo", "bar", "foo"});


        for (Map.Entry<String, Long> s : keyTopper.getSortedEntries()) {
            System.out.println(s.getKey() + "=" + (Long) s.getValue());
        }
    }
}

