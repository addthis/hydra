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
package com.addthis.hydra.minion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This lightweight thread periodically updates various minion metrics
 */
class HostMetricUpdater extends Thread {
    private static final Logger log = LoggerFactory.getLogger(HostMetricUpdater.class);

    private Minion minion;

    HostMetricUpdater(Minion minion) {
        this.minion = minion;
        setDaemon(true);
        start();
    }

    @Override public void run() {
        while (!minion.shutdown.get()) {
            try {
                Thread.sleep(Minion.hostMetricUpdaterInterval);
                minion.activeTaskHistogram.update(minion.activeTaskKeys.size());
                minion.diskFree.set(minion.rootDir.getFreeSpace());
            } catch (Exception ex) {
                if (!(ex instanceof InterruptedException)) {
                    log.warn("Exception during host metric update: " + ex, ex);
                }
            }
        }
    }
}
