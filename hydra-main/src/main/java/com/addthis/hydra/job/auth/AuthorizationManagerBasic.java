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
import java.util.Objects;

public class AuthorizationManagerBasic implements AuthorizationManager {

    private final AuthenticationManager authentication;

    public AuthorizationManagerBasic(AuthenticationManager authentication) {
        this.authentication = authentication;
    }

    @Override public boolean isAdmin(User user) {
        if (user == null) {
            return false;
        }
        List<String> adminUsers = authentication.adminUsers();
        List<String> adminGroups = authentication.adminGroups();
        if (adminUsers.contains(user)) {
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

    @Override public boolean isWritable(User user, WritableAsset asset) {
        if ((user == null) || (asset == null)) {
            return false;
        }
        if (Objects.equals(user.name(), asset.getOwner())) {
            return asset.isOwnerWritable();
        }
        String group = asset.getGroup();
        if (group != null) {
            if (user.groups().contains(group)) {
                return asset.isGroupWritable();
            }
        }
        return asset.isWorldWritable();
    }

}
