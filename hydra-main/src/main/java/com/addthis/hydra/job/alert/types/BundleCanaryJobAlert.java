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

import com.addthis.basis.util.Strings;

import com.addthis.codec.annotations.Time;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.alert.AbstractJobAlert;
import com.addthis.hydra.job.alert.JobAlertUtil;
import com.addthis.meshy.MeshyClient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BundleCanaryJobAlert extends AbstractJobAlert {

    @JsonProperty public final String canaryPath;
    @JsonProperty public final String canaryOps;
    @JsonProperty public final String canaryRops;
    @JsonProperty public final String canaryFilter;

    public BundleCanaryJobAlert(@Nullable @JsonProperty("alertId") String alertId,
                                @JsonProperty("description") String description,
                                @Time(TimeUnit.MINUTES) @JsonProperty("timeout") long timeout,
                                @Time(TimeUnit.MINUTES) @JsonProperty("delay") long delay,
                                @JsonProperty("email") String email,
                                @JsonProperty(value = "jobIds", required = true) List<String> jobIds,
                                @JsonProperty("canaryPath") String canaryPath,
                                @JsonProperty("canaryOps") String canaryOps,
                                @JsonProperty("canaryRops") String canaryRops,
                                @JsonProperty("canaryFilter") String canaryFilter,
                                @JsonProperty("lastAlertTime") long lastAlertTime,
                                @JsonProperty("activeJobs") Map<String, String> activeJobs,
                                @JsonProperty("activeTriggerTimes") Map<String, Long> activeTriggerTimes) {
        super(alertId, description, timeout, delay, email, jobIds, lastAlertTime, activeJobs, activeTriggerTimes);
        this.canaryPath = canaryPath;
        this.canaryOps = canaryOps;
        this.canaryRops = canaryRops;
        this.canaryFilter = canaryFilter;
    }

    @JsonProperty
    @Override
    public int getType() { return 6; }

    @JsonIgnore
    @Override protected String getTypeStringInternal() {
        return  "Bundle canary";
    }

    @Nullable @Override
    protected String testAlertActiveForJob(@Nullable MeshyClient meshClient, Job job, String previousErrorMessage) {
        try {
            String s = JobAlertUtil.evaluateQueryWithFilter(this, job.getId());
            consecutiveCanaryExceptionCount.set(0);
            return s;
        } catch (Exception ex) {
            return handleCanaryException(ex, previousErrorMessage);
        }
    }

    @Override public String isValid() {
        if (Strings.isEmpty(canaryPath)) {
            return "Canary path is empty";
        } else if (Strings.isEmpty(canaryFilter)) {
            return "Canary filter is empty";
        }
        try {
            CodecJSON.decodeString(BundleFilter.class, canaryFilter);
        } catch (Exception ex) {
            return "Error attempting to create bundle filter";
        }
        return null;
    }
}
