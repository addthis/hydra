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

import com.google.common.collect.ImmutableList;

/**
 * This authentication manager will create a user for any username and password
 * that is provided. Secret tokens are ignored.
 */
public class AuthenticationManagerAllowAll extends AuthenticationManager {

    @Override String login(String username, String password) {
        return "unused";
    }

    @Override boolean isAdmin(User user) {
        return true;
    }

    @Override User authenticate(String username, String secret) {
        return new StaticUser(username, ImmutableList.of(), "unused", "unused");
    }

    @Override protected User getUser(String username) {
        return new StaticUser(username, ImmutableList.of(), "unused", "unused");
    }

    @Override void logout(User user) {
        // do nothing
    }

    @Override ImmutableList<String> adminGroups() {
        return ImmutableList.of();
    }

    @Override ImmutableList<String> adminUsers() {
        return ImmutableList.of();
    }
}
