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
import java.util.Objects;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DefaultUser implements User {

    @Nonnull
    private final String name;

    @Nonnull
    private final List<String> groups;

    public DefaultUser(String name, List<String> groups) {
        this.name = name;
        this.groups = ImmutableList.copyOf(groups);
    }

    @Nonnull @Override public String name() {
        return name;
    }

    @Nonnull @Override public List<String> groups() {
        return groups;
    }

    static User join(User inner, User outer) {
        if (inner == null) {
            return outer;
        } else if (outer == null) {
            return inner;
        }
        if (!Objects.equals(inner.name(), outer.name())) {
            throw new IllegalArgumentException("Only users with identical usernames can be joined");
        }
        List<String> groups = ImmutableList.<String>builder()
                                           .addAll(inner.groups())
                                           .addAll(outer.groups()).build();
        return new DefaultUser(inner.name(), groups);
    }

}
