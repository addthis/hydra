package com.addthis.hydra.job.alert;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Group {
    public final String name;
    @Nullable public final String email;
    @Nullable public final String pagerEmail;
    @Nullable public final String webhookURL;

    @JsonCreator public Group(@JsonProperty(value = "name") String name,
                              @Nullable @JsonProperty(value = "email") String email,
                              @Nullable @JsonProperty(value = "pagerEmail") String pagerEmail,
                              @Nullable @JsonProperty(value = "webhookURL") String webhookURL) {
        this.name = name;
        this.email = email;
        this.pagerEmail = pagerEmail;
        this.webhookURL = webhookURL;
    }
}
