/*
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the "License").  You may not use this file except
 * in compliance with the License.
 * 
 * You can obtain a copy of the license at
 * http://www.opensource.org/licenses/cddl1.php
 * See the License for the specific language governing
 * permissions and limitations under the License.
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
