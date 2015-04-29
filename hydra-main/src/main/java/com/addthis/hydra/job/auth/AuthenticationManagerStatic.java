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

public class AuthenticationManagerStatic implements AuthenticationManager {

    private final ImmutableMap<String, User> users;

    private final ImmutableList<String> adminGroups;

    private final ImmutableList<String> adminUsers;

    @JsonCreator
    public AuthenticationManagerStatic(@JsonProperty("users") List<DefaultUser> users,
                                       @JsonProperty("adminGroups") List<String> adminGroups,
                                       @JsonProperty("adminUsers") List<String> adminUsers) {

        ImmutableMap.Builder<String, User> builder = ImmutableMap.<String, User> builder();
        for (User user : users) {
            builder.put(user.name(), user);
        }
        this.users = builder.build();
        this.adminGroups = ImmutableList.copyOf(adminGroups);
        this.adminUsers = ImmutableList.copyOf(adminUsers);
    }

    /**
     * In static authentication the user password and the user secret are
     * identical. Re-use the authentication method for login.
     */
    @Override public String login(String username, String password) {
        User match = authenticate(username, password);
        if (match != null) {
            return password;
        } else {
            return null;
        }
    }

    @Override public User authenticate(String username, String secret) {
        if ((username == null) || (secret == null)) {
            return null;
        }
        User match = users.get(username);
        if ((match != null) && (Objects.equals(match.secret(), secret))) {
            return match;
        }
        return null;
    }

    @Override public void logout(User user) {
        // do nothing
    }

    @Override public ImmutableList<String> adminGroups() {
        return adminGroups;
    }

    @Override public ImmutableList<String> adminUsers() {
        return adminUsers;
    }
}
