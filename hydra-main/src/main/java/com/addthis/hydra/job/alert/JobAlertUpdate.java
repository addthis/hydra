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
package com.addthis.hydra.job.alert;

import javax.annotation.Nonnull;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class JobAlertUpdate {

    @Nonnull @JsonProperty public final String error;

    @JsonProperty public final long timestamp;

    @Nonnull @JsonProperty public final JobAlertState state;

    @JsonCreator
    public JobAlertUpdate(@Nonnull @JsonProperty("error") String error,
                          @JsonProperty("timestamp") long timestamp,
                          @Nonnull @JsonProperty("state") JobAlertState state) {
        this.error = error;
        this.timestamp = timestamp;
        this.state = state;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof JobAlertUpdate)) return false;

        JobAlertUpdate that = (JobAlertUpdate) other;

        if (timestamp != that.timestamp) return false;
        if (!error.equals(that.error)) return false;
        if (!state.equals(that.state)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(error, timestamp, state);
    }
}
