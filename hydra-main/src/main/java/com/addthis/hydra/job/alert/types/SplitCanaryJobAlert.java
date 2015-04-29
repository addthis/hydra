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

import com.addthis.basis.util.LessStrings;

import com.addthis.codec.annotations.Time;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.alert.AbstractJobAlert;
import com.addthis.hydra.job.alert.JobAlertUtil;
import com.addthis.hydra.job.alert.SuppressChanges;
import com.addthis.meshy.MeshyClient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link AbstractJobAlert JobAlert} <span class="hydra-summary">alerts on disk usage of split jobs</span>.
 *
 * @user-reference
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SplitCanaryJobAlert extends AbstractJobAlert {

    /**
     * Path to the files that should be monitored.
     * Use glob expansion (wildcards) to match against multiple files.
     */
    @JsonProperty public final String canaryPath;

    /**
     * Alert if disk usage on any single host is less than threshold.
     */
    @JsonProperty public final long canaryConfigThreshold;

    public SplitCanaryJobAlert(@Nullable @JsonProperty("alertId") String alertId,
                               @JsonProperty("description") String description,
                               @Time(TimeUnit.MINUTES) @JsonProperty("delay") long delay,
                               @JsonProperty("email") String email,
                               @JsonProperty(value = "jobIds", required = true) List<String> jobIds,
                               @JsonProperty("suppressChanges") SuppressChanges suppressChanges,
                               @JsonProperty("canaryPath") String canaryPath,
                               @JsonProperty("canaryConfigThreshold") long canaryConfigThreshold,
                               @JsonProperty("lastAlertTime") long lastAlertTime,
                               @JsonProperty("activeJobs") Map<String, String> activeJobs,
                               @JsonProperty("activeTriggerTimes") Map<String, Long> activeTriggerTimes) {
        super(alertId, description, delay, email, jobIds, suppressChanges,
              lastAlertTime, activeJobs, activeTriggerTimes);
        this.canaryPath = canaryPath;
        this.canaryConfigThreshold  = canaryConfigThreshold;
    }

    @JsonIgnore
    @Override protected String getTypeStringInternal() {
        return "Split canary";
    }

    @Nullable @Override
    protected String testAlertActiveForJob(@Nullable MeshyClient meshClient, Job job, String previousErrorMessage) {
        // Strip off preceding slash, if it exists.
        StringBuilder message = new StringBuilder();
        String finalPath = canaryPath.startsWith("/") ? canaryPath.substring(1) : canaryPath;
        Map<String,Long> bytesPerHost = JobAlertUtil.getTotalBytesFromMesh(meshClient, job.getId(), finalPath);
        if (bytesPerHost.size() == 0) {
            return "No matching hosts found for path " + JobAlertUtil.meshLookupString(job.getId(), finalPath);
        }
        for (Map.Entry<String,Long> entry : bytesPerHost.entrySet()) {
            String host = entry.getKey();
            Long bytes = entry.getValue();
            if (bytes < canaryConfigThreshold) {
                message.append("For host " + host + " total bytes " + bytes + " < " + canaryConfigThreshold + "\n");
            }
        }
        if (message.length() == 0) {
            return null;
        } else {
            return message.toString();
        }
    }

    @Override public String isValid() {
        if (LessStrings.isEmpty(canaryPath)) {
            return "Canary path is empty";
        } else if (canaryConfigThreshold <= 0) {
            return "Canary config is not a positive integer";
        } else {
            return null;
        }
    }

}
