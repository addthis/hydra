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

package com.addthis.hydra.query.aggregate;

import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Parameter;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

public class AggregateConfig {

    // Maximum number of bundles to read before yielding event thread
    public static final int FRAME_READER_READS = Parameter.intValue("meshSourceAggregator.frameReader.reads", 10000);
    // Milliseconds to wait while polling various task sources
    public static final int FRAME_READER_POLL = Parameter.intValue("meshSourceAggregator.frameReader.poll", 0);

    static final boolean enableStragglerCheck = Parameter.boolValue("meshSourceAggregator.enableStragglerCheck", false);

    // Enables straggler check logic.
    static final int stragglerCheckPeriod = Parameter.intValue("meshSourceAggregator.stragglerCheckPeriodMillis", 1000);
    // Every 100 ms, check for stragglers
    static final double stragglerCheckHostPercentage = Double.parseDouble(Parameter.value("meshSourceAggregator.stragglerCheckHostPercentage", ".2"));
    // alternate straggler method
    static final boolean useStdDevStragglers = Parameter.boolValue("meshSourceAggregator.useStdDevStraggles", false);
    // A task could be a straggler if it's in the last 10% of tasks to return
    static final double stragglerCheckMeanRuntimeFactor = Double.parseDouble(Parameter.value("meshSourceAggregator.stragglerCheckMeanRuntimeFactor", "1.3"));

    /**
     * Identifies the number of standard deviations required to run stragglers when hosts are slow
     */
    static final double MULTIPLE_STD_DEVS = Double.parseDouble(Parameter.value("meshSourceAggregator.multipleStdDevs", "2"));
    static final AtomicBoolean exiting = new AtomicBoolean(false);
    /* metrics */
    static final Counter totalQueries = Metrics.newCounter(MeshSourceAggregator.class, "totalQueries");
    static final Counter totalStragglerCheckerRequests = Metrics.newCounter(MeshSourceAggregator.class, "totalStragglerCheckerRequests");
    static final Counter totalRetryRequests = Metrics.newCounter(MeshSourceAggregator.class, "totalRetryRequests");
}
