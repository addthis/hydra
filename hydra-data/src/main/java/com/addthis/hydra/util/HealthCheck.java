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
package com.addthis.hydra.util;


public class HealthCheck {

    private int maxFailures;
    private int currentFailures;
    private final Object counterLock;

    public HealthCheck(int maxFailures) {
        this.maxFailures = maxFailures;
        this.currentFailures = 0;
        this.counterLock = new Object();
    }

    public boolean check() {
        return true;
    }

    public void onPassedCheck() {
        // do nothing
    }

    public void onFailedCheck() {
        // do nothing
    }

    public void onFailure() {
        // do nothing
    }

    public final void passCheck() {
        synchronized (counterLock) {
            this.onPassedCheck();
            this.currentFailures = 0;
        }
    }

    public final void failCheck() {
        synchronized (counterLock) {
            this.currentFailures++;
            this.onFailedCheck();
            if (this.currentFailures >= this.maxFailures) {
                this.onFailure();
                this.currentFailures = 0;
            }
        }
    }

    public final boolean runCheck() {
        boolean result = this.check();
        if (result) {
            this.passCheck();
        } else {
            this.failCheck();
        }
        return result;
    }

    public final HealthCheckThread startHealthCheckThread(long pollingInterval, String threadName) {
        HealthCheckThread thread = new HealthCheckThread(this, pollingInterval, threadName);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }
}
