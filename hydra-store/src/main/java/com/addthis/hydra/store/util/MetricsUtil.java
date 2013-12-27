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
package com.addthis.hydra.store.util;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.stats.Snapshot;

public class MetricsUtil {

    public static String histogramToString(String title, Histogram histogram) {
        StringBuilder builder = new StringBuilder();
        builder.append("Statistics for histogram ");
        builder.append(title);
        builder.append("\n");
        builder.append("min: ");
        builder.append(histogram.min());
        builder.append("\n");
        builder.append("max: ");
        builder.append(histogram.max());
        builder.append("\n");
        builder.append("mean: ");
        builder.append(histogram.mean());
        builder.append("\n");
        Snapshot snapshot = histogram.getSnapshot();
        builder.append("50%: ");
        builder.append(snapshot.getMedian());
        builder.append("\n");
        builder.append("75%: ");
        builder.append(snapshot.get75thPercentile());
        builder.append("\n");
        builder.append("95%: ");
        builder.append(snapshot.get95thPercentile());
        builder.append("\n");
        builder.append("99%: ");
        builder.append(snapshot.get99thPercentile());
        builder.append("\n");
        builder.append("999%: ");
        builder.append(snapshot.get999thPercentile());
        builder.append("\n");
        builder.append("stdDev: ");
        builder.append(histogram.stdDev());
        builder.append("\n");
        builder.append("count: ");
        builder.append(histogram.count());
        builder.append("\n");
        return builder.toString();
    }

}
