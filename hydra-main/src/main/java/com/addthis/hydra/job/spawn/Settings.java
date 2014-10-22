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

/** Simple immutable wrapper of various system settings/states (mostly for generating ui json). */
public class Settings {

    public final String debug;
    public final boolean quiesce;
    public final String queryHost;
    public final String spawnHost;
    public final String disabled;
    public final int defaultReplicaCount;
    
    public Settings(
            String debug, 
            boolean quiesce, 
            String queryHost, 
            String spawnHost, 
            String disabled, 
            int defaultReplicaCount) {
        this.debug = debug;
        this.queryHost = queryHost;
        this.spawnHost = spawnHost;
        this.quiesce = quiesce;
        this.disabled = disabled;
        this.defaultReplicaCount = defaultReplicaCount;
    }
    
}
