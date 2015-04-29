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

import com.google.common.collect.ImmutableList;

/**
 * This authentication manager will create a user for any username and password
 * that is provided. Secret tokens are ignored. This authentication manager
 * does not generate any administrator groups. Use an authorization manager
 * to manage admin access, see {@link AuthorizationManagerAllowAll}.
 */
public class AuthenticationManagerAllowAll implements AuthenticationManager {

    private static class AllUser implements User {

        private final String name;

        AllUser(String name) {
            this.name = name;
        }

        @Nonnull @Override public String name() {
            return name;
        }

        @Nonnull @Override public ImmutableList<String> groups() {
            return ImmutableList.of();
        }

        @Nonnull @Override public String secret() {
            return "unused";
        }
    }

    @Override public String login(String username, String password) {
        return "unused";
    }

    @Override public User authenticate(String username, String secret) {
        return new AllUser(username);
    }

    @Override public void logout(User user) {
        // do nothing
    }

    @Override public ImmutableList<String> adminGroups() {
        return ImmutableList.of();
    }

    @Override public ImmutableList<String> adminUsers() {
        return ImmutableList.of();
    }
}
