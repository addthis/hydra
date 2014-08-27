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
package com.addthis.hydra.job.alert;

import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

public interface JobAlertManager {
    
    /** Enables periodic alert checking */
    void enableAlerts() throws Exception;

    /** Disables periodic alert checking */
    void disableAlerts() throws Exception;

    public void putAlert(String alertId, JobAlert alert);

    public void removeAlert(String alertId);

    public JSONArray fetchAllAlertsArray();

    public JSONObject fetchAllAlertsMap();

    public String getAlert(String alertId);

}
