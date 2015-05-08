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
 * Clients outside this package should not communicate
 * directly with AuthorizationManagers. They should use the
 * {@link PermissionsManager} API for authentication.
 */
abstract class AuthorizationManager {

    /**
     * Returns true if the user is able to update the asset.
     * The user is either authorized to update the asset through
     * the permissions model of this authorization manager,
     * or optionally the sudo token can be tested to grant sudo
     * access to update the asset.
     * @param user
     * @param sudoToken
     * @param asset
     * @return true if write permission is granted
     */
    abstract boolean isWritable(User user, String sudoToken, WritableAsset asset);

    /**
     * Returns true if the user is able to start or stop the asset.
     * The user is either authorized to update the asset through
     * the permissions model of this authorization manager,
     * or optionally the sudo token can be tested to grant sudo
     * access to update the asset.
     * @param user
     * @param sudoToken
     * @param asset
     * @return true if write permission is granted
     */
    abstract boolean isExecutable(User user, String sudoToken, ExecutableAsset asset);

    /**
     * Returns true if the user is able to modify permissions on the asset.
     * The authorization manager is allowed to be more permissive than the POSIX specification
     * which only allows the user or typically root to modify permissions.
     *
     * @param user
     * @param sudoToken
     * @param asset
     * @return true if write permission is granted
     */
    abstract boolean canModifyPermissions(User user, String sudoToken, WritableAsset asset);

    /**
     * Tests the provided sudo token and returns true
     * if the user is allowed to perform an administrative action,
     * such as quiescing the cluster.
     *
     * @param user
     * @param sudoToken
     * @return true if admin permission is granted
     */
    abstract boolean adminAction(User user, String sudoToken);

    /**
     * Grant a sudo token to the user or return null if no token
     * is granted. The {@code admin} parameter informs the authorization
     * manager if the user is an administrative user.
     *
     * @param user
     * @param admin
     * @return either token or null value to deny
     */
    abstract String sudo(User user, boolean admin);

    /**
     * Performs any logout activities such as clearing
     * the sudo cache.
     */
    abstract void logout(User user);

}
