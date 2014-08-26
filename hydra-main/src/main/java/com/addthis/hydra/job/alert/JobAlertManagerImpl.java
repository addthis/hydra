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

import static com.addthis.hydra.job.store.SpawnDataStoreKeys.SPAWN_COMMON_ALERT_PATH;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.hydra.job.store.SpawnDataStore;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobAlertManagerImpl implements JobAlertManager {

    private static final Logger log = LoggerFactory.getLogger(JobAlertManagerImpl.class);

    private static final long ALERT_REPEAT_MILLIS = Parameter.longValue("spawn.job.alert.repeat", 5 * 60 * 1000);
    private static final long ALERT_DELAY_MILLIS = Parameter.longValue("spawn.job.alert.delay", 60 * 1000);

    private final JobAlertRunner jobAlertRunner;
    private final SpawnDataStore spawnDataStore;

    public JobAlertManagerImpl(Spawn spawn, ScheduledExecutorService scheduledExecutorService) {
        this.spawnDataStore = spawn.getSpawnDataStore();
        this.jobAlertRunner = new JobAlertRunner(spawn, areAlertsEnabled());
        scheduleAlertScan(scheduledExecutorService);
    }

    private boolean areAlertsEnabled() {
        String alertsEnabled = null;
        try {
            alertsEnabled = spawnDataStore.get(SPAWN_COMMON_ALERT_PATH);
        } catch (Exception ex) {
            log.warn("Unable to read alerts status due to: {}", ex.getMessage());
        }
        return alertsEnabled == null || alertsEnabled.equals("") || alertsEnabled.equals("true");
    }

    private void scheduleAlertScan(ScheduledExecutorService scheduledExecutorService) {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    jobAlertRunner.scanAlerts();
                }
            }, ALERT_DELAY_MILLIS, ALERT_REPEAT_MILLIS, TimeUnit.MILLISECONDS);
            log.info("Alert scan scheduled: delay={}s, repeat={}s", ALERT_DELAY_MILLIS / 1000, ALERT_REPEAT_MILLIS / 1000);
        }
    }

    public void disableAlerts() throws Exception {
        spawnDataStore.put(SPAWN_COMMON_ALERT_PATH, "false");
        this.jobAlertRunner.disableAlerts();
    }

    public void enableAlerts() throws Exception {
        spawnDataStore.put(SPAWN_COMMON_ALERT_PATH, "true");
        this.jobAlertRunner.enableAlerts();
    }

    public void putAlert(String alertId, JobAlert alert) {
        jobAlertRunner.putAlert(alertId, alert);
    }

    public void removeAlert(String alertId) {
        jobAlertRunner.removeAlert(alertId);
    }

    public JSONArray fetchAllAlertsArray() {
        return jobAlertRunner.getAlertStateArray();
    }

    public JSONObject fetchAllAlertsMap() {
        return jobAlertRunner.getAlertStateMap();
    }

    public String getAlert(String alertId) {
        return jobAlertRunner.getAlert(alertId);
    }

}
