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
 * Static specification of users, administrator groups,
 * and administrator users. If an inner AuthenticationManager
 * is specified then the inner manager takes precedence for
 * authentication purposes.
 *
 * TODO: create an AuthenticationManager that continuously re-reads a file.
 * TODO: To allow for authentication changes without restarting cluster.
 *
 */
public class AuthenticationManagerStatic extends AuthenticationManager {

    private final ImmutableMap<String, StaticUser> users;

    private final ImmutableList<String> adminGroups;

    private final ImmutableList<String> adminUsers;

    private final AuthenticationManager inner;

    @JsonCreator
    public AuthenticationManagerStatic(@JsonProperty("users") List<StaticUser> users,
                                       @JsonProperty("adminGroups") List<String> adminGroups,
                                       @JsonProperty("adminUsers") List<String> adminUsers,
                                       @JsonProperty("inner")AuthenticationManager inner) {

        ImmutableMap.Builder<String, StaticUser> builder = ImmutableMap.<String, StaticUser> builder();
        if (users != null) {
            for (StaticUser user : users) {
                builder.put(user.name(), user);
            }
        }
        this.users = builder.build();
        this.adminGroups = (adminGroups == null) ? ImmutableList.of() : ImmutableList.copyOf(adminGroups);
        this.adminUsers = (adminUsers == null) ? ImmutableList.of() : ImmutableList.copyOf(adminUsers);
        this.inner = (inner == null) ? new AuthenticationManagerNoop() : inner;
    }

    @Override String login(String username, String password) {
        String token = inner.login(username, password);
        if ((token == null) && (users.containsKey(username))) {
            token = users.get(username).secret();
        }
        return token;
    }

    @Override User authenticate(String username, String secret) {
        if ((username == null) || (secret == null)) {
            return null;
        }
        User innerMatch = inner.authenticate(username, secret);
        StaticUser outerMatch = users.get(username);
        if ((innerMatch == null) && (!Objects.equals(outerMatch.secret(), secret))) {
            return null;
        } else {
            return DefaultUser.join(innerMatch, outerMatch);
        }
    }

    @Override void logout(User user) {
        if (user != null) {
            inner.logout(user);
        }
    }

    @Override ImmutableList<String> adminGroups() {
        return ImmutableList.<String>builder().addAll(inner.adminGroups()).addAll(adminGroups).build();
    }

    @Override ImmutableList<String> adminUsers() {
        return ImmutableList.<String>builder().addAll(inner.adminUsers()).addAll(adminUsers).build();
    }

}
