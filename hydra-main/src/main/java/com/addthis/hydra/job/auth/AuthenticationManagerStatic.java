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
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
class AuthenticationManagerStatic extends AuthenticationManager {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationManagerStatic.class);

    @Nonnull
    final ImmutableMap<String, StaticUser> users;

    @Nonnull
    final ImmutableList<String> adminGroups;

    @Nonnull
    final ImmutableList<String> adminUsers;

    @Nonnull
    final boolean requireSSL;

    @JsonCreator
    public AuthenticationManagerStatic(@Nonnull @JsonProperty("users") List<StaticUser> users,
                                       @Nonnull @JsonProperty("adminGroups") List<String> adminGroups,
                                       @Nonnull @JsonProperty("adminUsers") List<String> adminUsers,
                                       @JsonProperty(value = "requireSSL", required = true) boolean requireSSL) {

        ImmutableMap.Builder<String, StaticUser> builder = ImmutableMap.<String, StaticUser> builder();
        for (StaticUser user : users) {
            builder.put(user.name(), user);
        }
        this.users = builder.build();
        this.adminGroups = ImmutableList.copyOf(adminGroups);
        this.adminUsers = ImmutableList.copyOf(adminUsers);
        this.requireSSL = requireSSL;
        log.info("Registering static authentication");
    }

    @Override String login(String username, String password, boolean ssl) {
        if (requireSSL && !ssl) {
            return null;
        }
        User candidate = authenticate(username, password);
        if (candidate != null) {
            return password;
        } else {
            return null;
        }
    }

    @Override public boolean verify(String username, String password, boolean ssl) {
        if (requireSSL && !ssl) {
            return false;
        }
        User candidate = authenticate(username, password);
        return (candidate != null);
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

    @Override String sudoToken(String username) {
        StaticUser user = users.get(username);
        if (user != null) {
            return user.sudo();
        } else {
            return null;
        }
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
