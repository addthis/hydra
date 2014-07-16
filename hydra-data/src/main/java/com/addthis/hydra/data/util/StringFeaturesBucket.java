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


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;

public class StringFeaturesBucket implements Codable, FeaturesBucket {

    @FieldConfig(codable = true)
    private Map<String, Double> features;
    @FieldConfig(codable = true)
    private long hits = 0;

    public StringFeaturesBucket() {
        features = new HashMap<>();
    }

    public void addFeature(String feature) {
        Double weight = null;
        features.put(feature, (weight = features.get(feature)) != null ? weight + 1 : 1);
    }


    public long getHits() {
        return hits;
    }

    public void incrementHits(int n) {
        hits += n;
    }

    public void addFeatures(List<String> features) {
        for (String feature : features) {
            addFeature(feature);
        }
    }

    public void setHits(long hits) {
        this.hits = hits;
    }


    public String toString() {
        StringBuffer sb = new StringBuffer();

        for (String feature : features.keySet()) {
            if (sb.length() == 0) {
                sb.append(feature + ":" + features.get(feature));
            } else {
                sb.append("," + feature + ":" + features.get(feature));
            }
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        StringFeaturesBucket featuresBucket = new StringFeaturesBucket();
        featuresBucket.addFeature("foo");
        featuresBucket.addFeature("bar");
        featuresBucket.addFeature("foo");
        featuresBucket.addFeature("one");
        featuresBucket.addFeature("two");
        featuresBucket.addFeature("bar");
        featuresBucket.addFeature("bar");

        System.out.println(featuresBucket.toString());
    }
}
