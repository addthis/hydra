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
package com.addthis.hydra.job.alert.types;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.addthis.codec.annotations.Time;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.alert.AbstractJobAlert;
import com.addthis.hydra.job.alert.JobAlertUtil;
import com.addthis.meshy.MeshyClient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * If a value for a single host is above or below the threshold from the
 * mean value then raise an alert. The treshold is calculated as the
 * maximum of {@code tolerance} and ({@code sigma} multiplied by the
 * standard deviation).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileCountJobAlert extends AbstractJobAlert {

    private static final Logger log = LoggerFactory.getLogger(FileCountJobAlert.class);

    /**
     * Number of standard deviations away from the mean for an alert
     * to trigger. Default is 1.0.
     */
    @JsonProperty public final double sigma;
    /**
     * Absolute delta in number of files that are tolerated
     * before an alert triggers. Default is 0.0.
     */
    @JsonProperty public final int tolerance;
    @JsonProperty public final String canaryPath;

    public FileCountJobAlert(@Nullable @JsonProperty("alertId") String alertId,
                             @JsonProperty("description") String description,
                             @Time(TimeUnit.MINUTES) @JsonProperty("timeout") long timeout,
                             @Time(TimeUnit.MINUTES) @JsonProperty("delay") long delay,
                             @JsonProperty("email") String email,
                             @JsonProperty(value = "jobIds", required = true) List<String> jobIds,
                             @JsonProperty("suppressChanges") boolean suppressChanges,
                             @JsonProperty("lastAlertTime") long lastAlertTime,
                             @JsonProperty("activeJobs") Map<String, String> activeJobs,
                             @JsonProperty("activeTriggerTimes") Map<String, Long> activeTriggerTimes,
                             @JsonProperty("sigma") double sigma,
                             @JsonProperty("tolerance") int tolerance,
                             @JsonProperty("canaryPath") String canaryPath) {
        super(alertId, description, timeout, delay, email, jobIds, suppressChanges,
              lastAlertTime, activeJobs, activeTriggerTimes);
        this.sigma = sigma;
        this.tolerance = tolerance;
        this.canaryPath = canaryPath;
    }

    @JsonIgnore
    @Override protected String getTypeStringInternal() {
        return "Discrepancy in counts of log files across tasks";
    }

    private static final String ERROR_MESSAGE =
            "Host %s has %d log files which is %s than threshold %f" +
            " derived as mean value %f %s Math.max(%f multiplied by the" +
            " standard deviation %f, %d).\n";

    @Nullable @Override
    protected String testAlertActiveForJob(@Nullable MeshyClient meshClient, Job job, String previousErrorMessage) {
        Map<String, Integer> logCounts = JobAlertUtil.getFileCountPerTask(meshClient, job.getId(), canaryPath);
        log.debug("Log count map is {}", logCounts);
        if (logCounts.size() < 2) {
            return null;
        }
        StringBuilder errors = new StringBuilder();
        double mean = 0.0;
        double m2 = 0.0;
        int index = 0;
        for(Integer logCount : logCounts.values()) {
            index++;
            double delta = logCount - mean;
            mean += delta / index;
            m2 += delta * (logCount - mean);
        }
        double stddev = Math.sqrt(m2 / logCounts.size());
        log.debug("Mean is {} stddev is {}", mean, stddev);
        for(Map.Entry<String, Integer> entry : logCounts.entrySet()) {
            String hostUUID = entry.getKey();
            Integer logCount = entry.getValue();
            /**
             * The first and second conditions should never both be true.
             * But we test them independently to catch this illegal state.
             */
            double threshold = Math.max(sigma * stddev, tolerance);
            if (logCount < (mean - threshold)) {
                errors.append(String.format(ERROR_MESSAGE, hostUUID, logCount, "<",
                                            (mean - threshold), mean, "minus", sigma, stddev, tolerance));
            }
            if (logCount > (mean + threshold)) {
                errors.append(String.format(ERROR_MESSAGE, hostUUID, logCount, ">",
                                            (mean + threshold), mean, "plus", sigma, stddev, tolerance));
            }
        }
        String errorString = errors.toString();
        if (!errorString.isEmpty()) {
            return errorString;
        } else {
            return null;
        }
    }

    @Nullable @Override public String isValid() {
        if (sigma <= 0.0) {
            return "sigma parameter must be a positive value";
        }
        if (tolerance < 0) {
            return "tolerance parameter must be a non-negative value";
        }
        return null;
    }

}
