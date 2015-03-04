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
import com.addthis.hydra.job.JobState;
import com.addthis.hydra.job.alert.AbstractJobAlert;
import com.addthis.meshy.MeshyClient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OnCompleteJobAlert extends AbstractJobAlert {

    public OnCompleteJobAlert(@Nullable @JsonProperty("alertId") String alertId,
                              @JsonProperty("description") String description,
                              @Time(TimeUnit.MINUTES) @JsonProperty("timeout") long timeout,
                              @Time(TimeUnit.MINUTES) @JsonProperty("delay") long delay,
                              @JsonProperty("email") String email,
                              @JsonProperty(value = "jobIds", required = true) List<String> jobIds,
                              @JsonProperty("suppressChanges") boolean suppressChanges,
                              @JsonProperty("lastAlertTime") long lastAlertTime,
                              @JsonProperty("activeJobs") Map<String, String> activeJobs,
                              @JsonProperty("activeTriggerTimes") Map<String, Long> activeTriggerTimes) {
        super(alertId, description, timeout, delay, email, jobIds, suppressChanges,
              lastAlertTime, activeJobs, activeTriggerTimes);
    }

    @JsonIgnore
    @Override protected String getTypeStringInternal() {
        return "Task has Completed";
    }

    @Nullable @Override
    protected String testAlertActiveForJob(@Nullable MeshyClient meshClient, Job job, String previousErrorMessage) {
        if (job.getState() == JobState.IDLE) {
            return job.getState().name();
        } else {
            return null;
        }
    }

    @Override public String isValid() {
        return null;
    }

}
