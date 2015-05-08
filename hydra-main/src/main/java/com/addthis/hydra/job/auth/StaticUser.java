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
package com.addthis.hydra.job.auth;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StaticUser implements User {

    @Nonnull
    private final String name;

    @Nonnull
    private final ImmutableList<String> groups;

    @Nullable
    private final String secret;

    @Nullable
    private final String sudo;

    @JsonCreator
    public StaticUser(@JsonProperty("name") String name,
                      @JsonProperty("groups") List<String> groups,
                      @JsonProperty("secret") String secret,
                      @JsonProperty("sudo") String sudo) {
        this.name = name;
        this.groups = (groups == null) ? ImmutableList.of() : ImmutableList.copyOf(groups);
        this.secret = secret;
        this.sudo = sudo;
    }

    @Nonnull @Override public String name() {
        return name;
    }

    @Nonnull @Override public ImmutableList<String> groups() {
        return groups;
    }

    @Nullable public String secret() {
        return secret;
    }

    @Nullable public String sudo() {
        return sudo;
    }

}
