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

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Creates a static authentication manager of two users. One user with the
 * username of this process that has the same username and password. And one
 * administrative user with username and password "admin". This authentication
 * manager is intended for local testing.
 */
public class AuthenticationManagerLocalUser extends AuthenticationManager {

    private static final String USERNAME = System.getProperty("user.name");

    private final AuthenticationManagerStatic manager;

    @JsonCreator
    public AuthenticationManagerLocalUser() {
        StaticUser user = new StaticUser(USERNAME, ImmutableList.of(), USERNAME, USERNAME + "_sudotoken");
        StaticUser admin = new StaticUser("admin", ImmutableList.of(), "admin", "admin_sudotoken");
        manager = new AuthenticationManagerStatic(ImmutableList.of(user, admin), ImmutableList.of(),
                                                  ImmutableList.of("admin"), false);
    }

    @Override String login(String username, String password, boolean ssl) {
        return manager.login(username, password, ssl);
    }

    @Override public boolean verify(String username, String password, boolean ssl) {
        return manager.verify(username, password, ssl);
    }

    @Override User authenticate(String username, String secret) {
        return manager.authenticate(username, secret);
    }

    @Override protected User getUser(String username) {
        return manager.getUser(username);
    }

    @Override void logout(User user) {
        manager.logout(user);
    }

    @Override ImmutableList<String> adminGroups() {
        return manager.adminGroups();
    }

    @Override ImmutableList<String> adminUsers() {
        return manager.adminUsers();
    }
}
