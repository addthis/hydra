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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AuthorizationManagerBasic extends AuthorizationManager {

    private static final Logger log = LoggerFactory.getLogger(AuthorizationManagerBasic.class);

    @Nonnull
    private final TokenCache sudoCache;

    @Nonnull
    private final boolean groupModifyPermissions;

    @JsonCreator
    public AuthorizationManagerBasic(@JsonProperty("sudoCache") TokenCache sudoCache,
                                     @JsonProperty("groupModifyPermissions") boolean groupModifyPermissions) {
        this.sudoCache = sudoCache;
        this.groupModifyPermissions = groupModifyPermissions;
        log.info("Registering basic authorization");
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

    @Override boolean isExecutable(User user, String sudo, ExecutableAsset asset) {
        if ((user == null) || (asset == null)) {
            return false;
        } else if (sudoCache.get(user.name(), sudo)) {
            return true;
        } else {
            return isExecutable(user, asset);
        }
    }

    @Override boolean canModifyPermissions(User user, String sudo, WritableAsset asset) {
        String assetOwner = asset.getOwner();
        String assetGroup = asset.getGroup();
        if (Objects.equals(user.name(), assetOwner)) {
            return true;
        } else if (groupModifyPermissions && (assetGroup != null) && user.groups().contains(assetGroup)) {
            return true;
        } else {
            return isWritable(user, sudo, asset);
        }
    }

    @Override boolean adminAction(User user, String sudoToken) {
        if ((user == null) || (sudoToken == null)) {
            return false;
        } else {
            return sudoCache.get(user.name(), sudoToken);
        }
    }

    private boolean isWritable(User user, WritableAsset asset) {
        return testPermissions(user, asset, asset.isOwnerWritable(),
                               asset.isGroupWritable(), asset.isWorldWritable());
    }

    private boolean isExecutable(User user, ExecutableAsset asset) {
        return testPermissions(user, asset, asset.isOwnerExecutable(),
                               asset.isGroupExecutable(), asset.isWorldExecutable());
    }

    private boolean testPermissions(User user, OwnableAsset asset, boolean owner, boolean group, boolean world) {
        if (Objects.equals(user.name(), asset.getOwner())) {
            return owner;
        }
        String assetGroup = asset.getGroup();
        if (assetGroup != null) {
            if (user.groups().contains(assetGroup)) {
                return group;
            }
        }
        return world;
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
