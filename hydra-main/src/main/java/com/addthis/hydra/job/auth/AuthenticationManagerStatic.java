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

import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class AuthenticationManagerStatic extends AuthenticationManager {

    final ImmutableMap<String, StaticUser> users;

    final ImmutableList<String> adminGroups;

    final ImmutableList<String> adminUsers;

    @JsonCreator
    public AuthenticationManagerStatic(@JsonProperty("users") List<StaticUser> users,
                                       @JsonProperty("adminGroups") List<String> adminGroups,
                                       @JsonProperty("adminUsers") List<String> adminUsers) {

        ImmutableMap.Builder<String, StaticUser> builder = ImmutableMap.<String, StaticUser> builder();
        if (users != null) {
            for (StaticUser user : users) {
                builder.put(user.name(), user);
            }
        }
        this.users = builder.build();
        this.adminGroups = (adminGroups == null) ? ImmutableList.of() : ImmutableList.copyOf(adminGroups);
        this.adminUsers = (adminUsers == null) ? ImmutableList.of() : ImmutableList.copyOf(adminUsers);
    }

    @Override String login(String username, String password) {
        User candidate = authenticate(username, password);
        if (candidate != null) {
            return password;
        } else {
            return null;
        }
    }

    @Override User authenticate(String username, String secret) {
        if ((username == null) || (secret == null)) {
            return null;
        }
        StaticUser candidate = users.get(username);
        if ((candidate != null) && (secret.equals(candidate.secret()))) {
            return candidate;
        } else {
            return null;
        }
    }

    @Override protected User getUser(String username) {
        if (username == null) {
            return null;
        }
        return users.get(username);
    }

    @Override void logout(User user) {
        // do nothing
    }

    @Override ImmutableList<String> adminGroups() {
        return adminGroups;
    }

    @Override ImmutableList<String> adminUsers() {
        return adminUsers;
    }

}
