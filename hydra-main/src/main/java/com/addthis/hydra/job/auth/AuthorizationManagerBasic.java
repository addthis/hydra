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

import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthorizationManagerBasic extends AuthorizationManager {

    @Nonnull
    private final TokenCache sudoCache;

    @JsonCreator
    public AuthorizationManagerBasic(@JsonProperty("sudoCache") TokenCache sudoCache) {
        this.sudoCache = sudoCache;
    }

    @Override boolean isWritable(User user, String sudo, WritableAsset asset) {
        if ((user == null) || (asset == null)) {
            return false;
        } else if (sudoCache.get(user.name(), sudo)) {
            return true;
        } else {
            return isWritable(user, asset);
        }
    }

    private boolean isWritable(User user, WritableAsset asset) {
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

    @Override String sudo(User user, boolean admin) {
        if (admin) {
            UUID uuid = UUID.randomUUID();
            String token = uuid.toString();
            sudoCache.put(user.name(), token);
            return token;
        } else {
            return null;
        }
    }

    @Override void logout(User user) {
        sudoCache.remove(user.name());
    }

}
