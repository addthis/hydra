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

import com.google.common.collect.ImmutableList;

/**
 * Clients outside this package should not communicate
 * directly with AuthenticationManagers. They should use the
 * {@link PermissionsManager} API for authentication.
 */
abstract class AuthenticationManager {

    /**
     * Returns a non-null secret token if authentication
     * was successful. Or null if authentication failed.
     * An authentication manager can choose to deny requests
     * that are not transmitted over ssl. At this point the password
     * has already been transmitted but denying the request may
     * be preferable to encouraging this behavior.
     *
     * @param username
     * @param password
     * @param ssl
     * @return non-null secret if authentication succeeded
     */
    abstract String login(String username, String password, boolean ssl);

    /**
     * Verifies the username and password are correct.
     */
    public abstract boolean verify(String username, String password, boolean ssl);

    /**
     * Return the user object if the username and secret token are valid.
     *
     * @param username
     * @param secret
     * @return
     */
    abstract User authenticate(String username, String secret);


    /**
     * Bypasses authentication. Protected visibility should only
     * be used by internal methods.
     *
     * @param username
     * @return
     */
    protected abstract User getUser(String username);

    /**
     * Optionally provides a sudo token for the user
     * or return null to delegate sudo token generation to the
     * authorization manager.
     *
     * @param username
     * @return
     */
    abstract String sudoToken(String username);

    /**
     * Logout the user from the authentication manager. The secret
     * token for the user should be invalidated.
     *
     * @param user
     */
    abstract void logout(User user);

    abstract ImmutableList<String> adminGroups();

    abstract ImmutableList<String> adminUsers();

    boolean isAdmin(User user) {
        if (user == null) {
            return false;
        }
        List<String> adminUsers = adminUsers();
        List<String> adminGroups = adminGroups();
        if (adminUsers.contains(user.name())) {
            return true;
        }
        List<String> groups = user.groups();
        for (String group : groups) {
            if (adminGroups.contains(group)) {
                return true;
            }
        }
        return false;
    }

}
