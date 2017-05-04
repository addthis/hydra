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

import com.fasterxml.jackson.annotation.JsonProperty;

public class HealthCheckResult {

    private boolean dataStoreOK;
    private boolean alertCheckOK;

    @JsonProperty public boolean isEverythingOK() {
        return dataStoreOK && alertCheckOK;
    }

    @JsonProperty public boolean isDataStoreOK() {
        return dataStoreOK;
    }

    @JsonProperty public boolean isAlertCheckOK() {
        return alertCheckOK;
    }

    public HealthCheckResult setDataStoreOK(boolean dataStoreOK) {
        this.dataStoreOK = dataStoreOK;
        return this;
    }

    public HealthCheckResult setAlertCheckOK(boolean alertCheckOK) {
        this.alertCheckOK = alertCheckOK;
        return this;
    }
}
