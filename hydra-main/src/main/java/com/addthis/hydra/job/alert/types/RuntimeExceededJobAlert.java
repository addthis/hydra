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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.addthis.codec.annotations.Time;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobState;
import com.addthis.hydra.job.alert.AbstractJobAlert;
import com.addthis.hydra.job.alert.SuppressChanges;
import com.addthis.meshy.MeshyClient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link AbstractJobAlert JobAlert} <span class="hydra-summary">alerts when the job runtime is exceeded</span>.
 *
 * @user-reference
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RuntimeExceededJobAlert extends AbstractJobAlert {

    /**
     * Maximum number of minutes job can execute.
     */
    @JsonProperty public final long timeout;


    public RuntimeExceededJobAlert(@Nullable @JsonProperty("alertId") String alertId,
                                   @JsonProperty("description") String description,
                                   @Time(TimeUnit.MINUTES) @JsonProperty("timeout") long timeout,
                                   @Time(TimeUnit.MINUTES) @JsonProperty("delay") long delay,
                                   @JsonProperty("email") String email,
                                   @JsonProperty(value = "jobIds", required = true) List<String> jobIds,
                                   @JsonProperty("suppressChanges") SuppressChanges suppressChanges,
                                   @JsonProperty("lastAlertTime") long lastAlertTime,
                                   @JsonProperty("activeJobs") Map<String, String> activeJobs,
                                   @JsonProperty("activeTriggerTimes") Map<String, Long> activeTriggerTimes) {
        super(alertId, description, delay, email, jobIds, suppressChanges,
              lastAlertTime, activeJobs, activeTriggerTimes);
        this.timeout = timeout;
    }

    @JsonIgnore
    @Override protected String getTypeStringInternal() {
        return "Task runtime exceeded";
    }

    @Nullable @Override
    protected String testAlertActiveForJob(@Nullable MeshyClient meshClient, Job job, String previousErrorMessage) {
        long currentTime = System.currentTimeMillis();
        if ((job.getState() == JobState.RUNNING) && (job.getStartTime() != null)) {
            long runningTime = currentTime - job.getStartTime();
            if (runningTime > TimeUnit.MINUTES.toMillis(timeout)) {
                return "Job startTime is " + new Date(job.getStartTime());
            }
        }
        return null;
    }

    @Override public String isValid() {
        return null;
    }

}
