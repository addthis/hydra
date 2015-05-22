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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AuthenticationManagerDenyAll extends AuthenticationManager {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationManagerDenyAll.class);

    @JsonCreator
    public AuthenticationManagerDenyAll() {
        log.info("Registering deny all authentication");
    }

    @Override String login(String username, String password, boolean ssl) {
        return null;
    }

    @Override public boolean verify(String username, String password, boolean ssl) {
        return false;
    }

    @Override User authenticate(String username, String secret) {
        return null;
    }

    @Override protected User getUser(String username) {
        return null;
    }

    @Override void logout(User user) {
    }

    @Override String sudoToken(String username) {
        return null;
    }

    @Override boolean isAdmin(User user) {
        return false;
    }

    @Override ImmutableList<String> adminGroups() {
        return ImmutableList.of();
    }

    @Override ImmutableList<String> adminUsers() {
        return ImmutableList.of();
    }
}
