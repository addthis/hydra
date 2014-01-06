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

import com.addthis.basis.util.JitterClock;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;
import com.addthis.maljson.JSONObject;

/**
 * Bean to hold a job specific alert
 */
public class JobAlert implements Codec.Codable {

    //Alert types
    public static final int ON_ERROR = 0;
    public static final int ON_COMPLETE = 1;
    public static final int RUNTIME_EXCEEDED = 2;
    public static final int REKICK_TIMEOUT = 3;

    @Codec.Set(codable = true)
    private long lastAlertTime;
    @Codec.Set(codable = true)
    private int type;
    @Codec.Set(codable = true)
    private int timeout;
    @Codec.Set(codable = true)
    private String email;
    @Codec.Set(codable = true)
    private String[] jobIds;

    public JobAlert() {
        this.lastAlertTime = -1;
        this.type = 0;
        this.timeout = 0;
        this.email = "";
    }

    public JobAlert(int type, int timeout, String email, String[] jobIds) {
        this.lastAlertTime = -1;
        this.type = type;
        this.timeout = timeout;
        this.email = email;
        this.jobIds = jobIds;
    }

    public boolean isOnError() {
        return type == ON_ERROR;
    }

    public boolean isOnComplete() {
        return type == ON_COMPLETE;
    }

    public boolean isRuntimeExceeded() {
        return type == RUNTIME_EXCEEDED;
    }

    public boolean isRekickTimeout() {
        return type == REKICK_TIMEOUT;
    }

    public boolean hasAlerted() {
        return this.lastAlertTime > 0;
    }

    public void alerted() {
        this.lastAlertTime = JitterClock.globalTime();
    }

    public void clear() {
        this.lastAlertTime = -1;
    }

    public long getLastAlertTime() {
        return lastAlertTime;
    }

    public void setLastAlertTime(long lastAlertTime) {
        this.lastAlertTime = lastAlertTime;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String[] getJobIds() {
        return jobIds;
    }

    public void setJobIds(String[] jobIds) {
        this.jobIds = jobIds;
    }

    public JSONObject toJSON() throws Exception {
        return CodecJSON.encodeJSON(this);
    }

    @Override
    public String toString() {
        try {
            return CodecJSON.encodeString(this, true);
        } catch (Exception e) {
            return super.toString();
        }
    }
}
