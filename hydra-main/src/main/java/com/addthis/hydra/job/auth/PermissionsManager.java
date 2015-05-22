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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class around authentication and authorization. Provides convenience methods
 * for authorization operations that must first be authenticated.
 */
public final class PermissionsManager {

    private static final Logger log = LoggerFactory.getLogger(PermissionsManager.class);

    private final AuthenticationManager authentication;

    private final AuthorizationManager authorization;

    private static PermissionsManager ALLOW_ALL = new PermissionsManager(
            new AuthenticationManagerAllowAll(), new AuthorizationManagerAllowAll());

    public static PermissionsManager createManagerAllowAll() {
        return ALLOW_ALL;
    }

    @JsonCreator
    public PermissionsManager(@JsonProperty(value = "authentication", required = true) AuthenticationManager authentication,
                              @JsonProperty(value = "authorization", required = true) AuthorizationManager authorization) {
        this.authentication = authentication;
        this.authorization = authorization;
    }

    /**
     * Authorization checks that are common to all endpoints.
     * If the user is null then authorization fails. If the user's
     * sudo token matches the static sudo token returned by the
     * authentication manager then authorization succeeds.
     *
     * @param user
     * @param sudo
     * @return outcome of authorization or null to continue processing
     */
    private Boolean authorizationCommon(User user, String sudo) {
        if (user == null) {
            return Boolean.FALSE;
        } else if ((sudo != null) && sudo.equals(authentication.sudoToken(user.name()))) {
            return Boolean.TRUE;
        }
        return null;
    }

    public boolean isWritable(String username, String secret, String sudo, WritableAsset asset) {
        User user = authentication.authenticate(username, secret);
        Boolean result = authorizationCommon(user, sudo);
        if (result != null) return result;
        return authorization.isWritable(user, sudo, asset);
    }

    public boolean isExecutable(String username, String secret, String sudo, ExecutableAsset asset) {
        User user = authentication.authenticate(username, secret);
        Boolean result = authorizationCommon(user, sudo);
        if (result != null) return result;
        return authorization.isExecutable(user, sudo, asset);
    }

    public boolean canModifyPermissions(String username, String secret, String sudo, WritableAsset asset) {
        User user = authentication.authenticate(username, secret);
        Boolean result = authorizationCommon(user, sudo);
        if (result != null) return result;
        return authorization.canModifyPermissions(user, sudo, asset);
    }

    public boolean adminAction(String username, String secret, String sudo) {
        User user = authentication.authenticate(username, secret);
        Boolean result = authorizationCommon(user, sudo);
        if (result != null) return result;
        return authorization.adminAction(user, sudo);
    }

    public User authenticate(String username, String secret) {
        return authentication.authenticate(username, secret);
    }

    public String login(String username, String password, boolean ssl) {
        return authentication.login(username, password, ssl);
    }

    public String sudo(String username, String password, boolean ssl) {
        boolean success = authentication.verify(username, password, ssl);
        User user = success ? authentication.getUser(username) : null;
        if (user == null) {
            return null;
        } else {
            String staticToken = authentication.sudoToken(username);
            if (staticToken != null) {
                return staticToken;
            } else {
                return authorization.sudo(user, authentication.isAdmin(user));
            }
        }
    }

    public void logout(String username, String secret) {
        User user = authentication.authenticate(username, secret);
        if (user != null) {
            authentication.logout(user);
            authorization.logout(user);
        }
    }

}
