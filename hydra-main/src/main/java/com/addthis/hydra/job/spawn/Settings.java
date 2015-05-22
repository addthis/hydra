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
package com.addthis.hydra.job.spawn;

import com.google.common.base.Preconditions;

/** Simple immutable wrapper of various system settings/states (mostly for generating ui json). */
public class Settings {

    public final String debug;
    public final boolean quiesce;
    public final String queryHost;
    public final String spawnHost;
    public final String disabled;
    public final int defaultReplicaCount;
    public final boolean sslDefault;
    public final int authTimeout;
    public final int sudoTimeout;

    public static class Builder {
        private String debug;
        private Boolean quiesce;
        private String queryHost;
        private String spawnHost;
        private String disabled;
        private Integer defaultReplicaCount;
        private Boolean sslDefault;
        private Integer authTimeout;
        private Integer sudoTimeout;

        public Builder setDebug(String debug) {
            this.debug = debug;
            return this;
        }

        public Builder setQuiesce(Boolean quiesce) {
            this.quiesce = quiesce;
            return this;
        }

        public Builder setQueryHost(String queryHost) {
            this.queryHost = queryHost;
            return this;
        }

        public Builder setSpawnHost(String spawnHost) {
            this.spawnHost = spawnHost;
            return this;
        }

        public Builder setDisabled(String disabled) {
            this.disabled = disabled;
            return this;
        }

        public Builder setDefaultReplicaCount(Integer defaultReplicaCount) {
            this.defaultReplicaCount = defaultReplicaCount;
            return this;
        }

        public Builder setSslDefault(Boolean sslDefault) {
            this.sslDefault = sslDefault;
            return this;
        }

        public Builder setAuthTimeout(Integer authTimeout) {
            this.authTimeout = authTimeout;
            return this;
        }

        public Builder setSudoTimeout(Integer sudoTimeout) {
            this.sudoTimeout = sudoTimeout;
            return this;
        }

        public Settings build() {
            Preconditions.checkArgument(quiesce != null, "parameter quiesce must be specified");
            Preconditions.checkArgument(queryHost != null, "parameter queryHost must be specified");
            Preconditions.checkArgument(spawnHost != null, "parameter spawnHost must be specified");
            Preconditions.checkArgument(disabled != null, "parameter disabled must be specified");
            Preconditions.checkArgument(defaultReplicaCount != null, "parameter defaultReplicaCount must be specified");
            Preconditions.checkArgument(sslDefault != null, "parameter sslDefault must be specified");
            Preconditions.checkArgument(authTimeout != null, "parameter authTimeout must be specified");
            Preconditions.checkArgument(sudoTimeout != null, "parameter sudoTimeout must be specified");
            return new Settings(debug, quiesce, queryHost, spawnHost, disabled,
                                defaultReplicaCount, sslDefault, authTimeout, sudoTimeout);
        }
    }

    private Settings(
            String debug, 
            boolean quiesce, 
            String queryHost, 
            String spawnHost, 
            String disabled, 
            int defaultReplicaCount,
            boolean sslDefault,
            int authTimeout,
            int sudoTimeout) {
        this.debug = debug;
        this.queryHost = queryHost;
        this.spawnHost = spawnHost;
        this.quiesce = quiesce;
        this.disabled = disabled;
        this.defaultReplicaCount = defaultReplicaCount;
        this.sslDefault = sslDefault;
        this.authTimeout = authTimeout;
        this.sudoTimeout = sudoTimeout;
    }
    
}
