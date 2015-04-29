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

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DefaultUser implements User {

    @Nonnull
    private final String name;

    @Nonnull
    private final ImmutableList<String> groups;

    @Nonnull
    public final String secret;

    @JsonCreator
    public DefaultUser(@JsonProperty("name") String name,
                       @JsonProperty("groups") List<String> groups,
                       @JsonProperty("secret") String secret) {
        this.name = name;
        this.groups = ImmutableList.copyOf(groups);
        this.secret = secret;
    }

    @Nonnull @Override public String name() {
        return name;
    }

    @Nonnull @Override public ImmutableList<String> groups() {
        return groups;
    }

    @Nonnull @Override public String secret() {
        return secret;
    }
}
