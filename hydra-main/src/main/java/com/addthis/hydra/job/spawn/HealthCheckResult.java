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

public class HealthCheckResult {

    private boolean dataStoreOK;

    public boolean isEverythingOK() {
        return dataStoreOK;
    }

    public boolean isDataStoreOK() {
        return dataStoreOK;
    }

    public HealthCheckResult setDataStoreOK(boolean dataStoreOK) {
        this.dataStoreOK = dataStoreOK;
        return this;
    }

}
