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


import java.util.List;

import com.addthis.codec.Codec;

public class IndexedFeaturesBucket implements Codec.Codable, FeaturesBucket {

    @Codec.Set(codable = true)
    private int[] featuresIndex;
    @Codec.Set(codable = true)
    private int[] featuresWeight;
    @Codec.Set(codable = true)
    private int SIZE = 1000;
    @Codec.Set(codable = true)
    private int currentIndex = 0;
    @Codec.Set(codable = true)
    private long hits = 0;

    public IndexedFeaturesBucket() {
        featuresIndex = new int[SIZE];
        featuresWeight = new int[SIZE];
    }

    public void addFeature(String feature) {
        int featureIndex = Integer.parseInt(feature);

        for (int i = 0; i < featuresIndex.length; i++) {
            if (featuresIndex[i] == featureIndex) {
                featuresWeight[i] += 1;
                return;
            }
        }

        if (currentIndex < featuresIndex.length) {
            featuresIndex[currentIndex] = featureIndex;
            featuresWeight[currentIndex] = 1;
            currentIndex++;
        } else {
            // extend arrays
            featuresIndex = copy(featuresIndex, featuresIndex.length + SIZE);
            featuresWeight = copy(featuresWeight, featuresWeight.length + SIZE);

            featuresIndex[currentIndex] = featureIndex;
            featuresWeight[currentIndex] = 1;
            currentIndex++;
        }
    }

    private int[] copy(int[] oldArray, int newArraySize) {
        int[] temp = new int[newArraySize];

        for (int i = 0; i < oldArray.length; i++) {
            temp[i] = oldArray[i];
        }

        return temp;
    }

    public long getHits() {
        return hits;
    }

    public void incrementHits(int k) {
        hits += k;
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

        for (int i = 0; i < featuresIndex.length && featuresIndex[i] != 0; i++) {
            if (sb.length() == 0) {
                sb.append(featuresIndex[i] + ":" + featuresWeight[i]);
            } else {
                sb.append("," + featuresIndex[i] + ":" + featuresWeight[i]);
            }
        }

        return sb.toString();
    }

    public static void main(String args[]) {
        IndexedFeaturesBucket featuresBucket = new IndexedFeaturesBucket();
        featuresBucket.addFeature("1");
        featuresBucket.addFeature("1");
        featuresBucket.addFeature("1");
        featuresBucket.addFeature("2");
        featuresBucket.addFeature("2");
        featuresBucket.addFeature("3");
        featuresBucket.addFeature("4");

        System.out.println(featuresBucket.toString());
    }
}