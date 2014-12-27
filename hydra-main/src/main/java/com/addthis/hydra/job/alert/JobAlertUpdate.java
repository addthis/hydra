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
