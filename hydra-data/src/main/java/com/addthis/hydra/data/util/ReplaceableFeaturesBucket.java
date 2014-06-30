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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;

public class ReplaceableFeaturesBucket implements Codable {

    @FieldConfig(codable = true)
    private Map<String, Double> featuresMap;
    @FieldConfig(codable = true)
    private long hits = 0;

    public ReplaceableFeaturesBucket() {
        featuresMap = new HashMap<String, Double>();
    }

    public void addFeature(String feature, double initialWeight) {
        Double weight = null;
        featuresMap.put(feature, (weight = featuresMap.get(feature)) != null ? weight + initialWeight : initialWeight);
    }


    public long getHits() {
        return hits;
    }

    public void incrementHits(int n) {
        hits += n;
    }

    public void addFeatures(List<String> features) {
        for (String feature : features) {
            addFeature(feature, 1);
        }
    }

    public void augmentExistingFeatures(String original, List<String> values) {
        Double count = null;

        // only if we an existing key
        if ((count = featuresMap.get(original)) != null) {
            for (String value : values) {
                if (!featuresMap.containsKey(value)) {
                    addFeature(value, count);
                }
            }
        }
    }

    public void setHits(long hits) {
        this.hits = hits;
    }


    public String toString() {
        StringBuffer sb = new StringBuffer();

        for (String feature : featuresMap.keySet()) {
            if (sb.length() == 0) {
                sb.append(feature + ":" + featuresMap.get(feature));
            } else {
                sb.append("," + feature + ":" + featuresMap.get(feature));
            }
        }

        return sb.toString();
    }

    public static void main(String args[]) {
        ReplaceableFeaturesBucket featuresBucket = new ReplaceableFeaturesBucket();
        featuresBucket.addFeature("foo");
        featuresBucket.addFeature("bar");
        featuresBucket.addFeature("foo");
        featuresBucket.addFeature("one");
        featuresBucket.addFeature("one");
        featuresBucket.addFeature("two");
        featuresBucket.addFeature("bar");
        featuresBucket.addFeature("bar");

        featuresBucket.augmentExistingFeatures("one", Arrays.asList("4444", "24444", "8888"));
        System.out.println(featuresBucket.toString());

        featuresBucket.augmentExistingFeatures("one", Arrays.asList("4444", "24444", "8888"));
        System.out.println(featuresBucket.toString());
    }

    private void addFeature(String key) {
        addFeature(key, 1);
    }
}