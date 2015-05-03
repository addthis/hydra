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

/**
 * Wrapper class around authentication and authorization. Provides convenience methods
 * for authorization operations that must first be authenticated.
 */
public final class PermissionsManager {

    private final AuthenticationManager authentication;

    private final AuthorizationManager authorization;

    private static PermissionsManager ALLOW_ALL = new PermissionsManager(
            new AuthenticationManagerAllowAll(), new AuthorizationManagerAllowAll());

    public static PermissionsManager createManagerAllowAll() {
        return ALLOW_ALL;
    }

    public PermissionsManager(AuthenticationManager authentication, AuthorizationManager authorization) {
        this.authentication = authentication;
        this.authorization = authorization;
    }

    public boolean isWritable(String username, String secret, String sudo, WritableAsset asset) {
        User user = authentication.authenticate(username, secret);
        if (user == null) {
            return false;
        }
        return authorization.isWritable(user, sudo, asset);
    }

    public User authenticate(String username, String secret) {
        return authentication.authenticate(username, secret);
    }

    public String login(String username, String password) {
        return authentication.login(username, password);
    }

    public String sudo(String username, String secret) {
        User user = authentication.authenticate(username, secret);
        if (user == null) {
            return null;
        } else {
            return authorization.sudo(user, authentication.isAdmin(user));
        }
    }

    public void logout(User user) {
        authentication.logout(user);
        authorization.logout(user);
    }

    public boolean isAdmin(String username, String secret) {
        User user = authentication.authenticate(username, secret);
        if (user == null) {
            return false;
        }
        return authentication.isAdmin(user);
    }

}
