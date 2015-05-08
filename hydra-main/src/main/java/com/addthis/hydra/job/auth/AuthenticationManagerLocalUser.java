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

import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates an authentication manager of two users. One user with the
 * username of this process that has the same username and password. And one
 * administrative user with username and password "admin". This authentication
 * manager is intended for local testing. Actual UUID tokens are created to simulate
 * a real authentication manager.
 */
class AuthenticationManagerLocalUser extends AuthenticationManager {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationManagerLocalUser.class);

    private static final String USERNAME = System.getProperty("user.name");
    private static final ImmutableSet<String> USERS = ImmutableSet.of(USERNAME, "admin");

    private final TokenCache tokenCache;
    private final User basicUser;
    private final User adminUser;
    private final ImmutableMap<String, User> users;


    @JsonCreator
    public AuthenticationManagerLocalUser(@JsonProperty(value = "tokenCache", required = true) TokenCache tokenCache) {
        this.tokenCache = tokenCache;
        this.basicUser = new DefaultUser(USERNAME, ImmutableList.of());
        this.adminUser = new DefaultUser("admin", ImmutableList.of());
        this.users = ImmutableMap.of(USERNAME, basicUser, "admin", adminUser);
        log.info("Registering local user authentication");
    }

    @Override String login(String username, String password, boolean ssl) {
        if ((username == null) || (password == null)) {
            return null;
        }
        if (Objects.equals(username, password) && USERS.contains(username)) {
            UUID uuid = UUID.randomUUID();
            String token = uuid.toString();
            tokenCache.put(username, token);
            return token;
        } else {
            return null;
        }

    }

    @Override public boolean verify(String username, String password, boolean ssl) {
        if ((username == null) || (password == null)) {
            return false;
        }
        return Objects.equals(username, password) && USERS.contains(username);
    }

    @Override User authenticate(String username, String secret) {
        if ((username == null) || (secret == null)) {
            return null;
        }
        if (tokenCache.get(username, secret)) {
            return users.get(username);
        } else {
            return null;
        }
    }

    @Override protected User getUser(String username) {
        return users.get(username);
    }

    @Override String sudoToken(String username) {
        return null;
    }

    @Override void logout(User user) {
        tokenCache.remove(user.name());
    }

    @Override ImmutableList<String> adminGroups() {
        return ImmutableList.of();
    }

    @Override ImmutableList<String> adminUsers() {
        return ImmutableList.of("admin");
    }
}
