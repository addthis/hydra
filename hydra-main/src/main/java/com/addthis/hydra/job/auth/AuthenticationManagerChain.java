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

public class AuthenticationManagerChain implements AuthenticationManager {

    @Nonnull
    private final ImmutableList<AuthenticationManager> managers;

    @JsonCreator
    public AuthenticationManagerChain(@JsonProperty("managers") @Nonnull List<AuthenticationManager> managers) {
        this.managers = ImmutableList.copyOf(managers);
    }


    @Override public String login(String username, String password) {
        for (AuthenticationManager manager : managers) {
            String secret = manager.login(username, password);
            if (secret != null) {
                return secret;
            }
        }
        return null;
    }

    @Override public User authenticate(String username, String secret) {
        for (AuthenticationManager manager : managers) {
            User user = manager.authenticate(username, secret);
            if (user != null) {
                return user;
            }
        }
        return null;
    }

    @Override public void logout(User user) {
        for (AuthenticationManager manager : managers) {
            manager.logout(user);
        }
    }

    @Override public ImmutableList<String> adminGroups() {
        ImmutableList.Builder<String> builder = ImmutableList.<String>builder();
        for (AuthenticationManager manager : managers) {
            builder.addAll(manager.adminGroups());
        }
        return builder.build();
    }

    @Override public ImmutableList<String> adminUsers() {
        ImmutableList.Builder<String> builder = ImmutableList.<String>builder();
        for (AuthenticationManager manager : managers) {
            builder.addAll(manager.adminUsers());
        }
        return builder.build();
    }
}
