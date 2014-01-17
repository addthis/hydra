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
package com.addthis.hydra.job.store;

/**
 * Predefined keys for {@link SpawnDataStore}.
 */
public class SpawnDataStoreKeys {

    public static final String SPAWN_COMMON_MACRO_PATH = "/spawn/common/macro";
    public static final String SPAWN_COMMON_COMMAND_PATH = "/spawn/common/command";
    public static final String SPAWN_QUEUE_PATH = "/spawn/queue";
    public static final String SPAWN_BALANCE_PARAM_PATH = "/spawn/balanceparam";
    public static final String MINION_UP_PATH = "/minion/up";
    public static final String MINION_DEAD_PATH = "/minion/dead";
    public static final String SPAWN_JOB_CONFIG_PATH = "/spawn/jobs";
    public static final String SPAWN_COMMON_ALERT_PATH = "/spawn/common/alerts";

    /* Marker to make sure we import legacy alerts from Jobs exactly once */
    public static final String SPAWN_COMMON_ALERT_LOADED_LEGACY = "/spawn/common/alerts/_loaded_legacy";

}
