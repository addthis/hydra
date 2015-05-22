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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AuthorizationManagerAllowAll extends AuthorizationManager {

    private static final Logger log = LoggerFactory.getLogger(AuthorizationManagerAllowAll.class);

    @JsonCreator
    public AuthorizationManagerAllowAll() {
        log.info("Registering allow all authorization");
    }

    @Override boolean isWritable(User user, String sudoToken, WritableAsset asset) {
        return true;
    }

    @Override boolean isExecutable(User user, String sudoToken, ExecutableAsset asset) {
        return true;
    }

    @Override boolean canModifyPermissions(User user, String sudoToken, WritableAsset asset) {
        return true;
    }

    @Override boolean adminAction(User user, String sudoToken) {
        return true;
    }

    @Override String sudo(User user, boolean admin) {
        return "unused";
    }

    @Override void logout(User user) {
        // do nothing
    }
}
