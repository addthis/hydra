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


import java.io.File;

import java.util.ArrayList;
import java.util.List;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.Minion;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class MinionWriteableDiskCheck extends WriteableDiskCheck {

    private static final int MINION_DISK_CHECK_MAXFAILURES = Parameter.intValue("minion.disk.check.maxFailures", 3);
    private static final int MINION_DISK_CHECK_INTERVAL = Parameter.intValue("minion.disk.check.interval", 1000);

    private Minion minion;
    private static Logger log = LoggerFactory.getLogger(MinionWriteableDiskCheck.class);


    public MinionWriteableDiskCheck(Minion minion) {
        super(MINION_DISK_CHECK_MAXFAILURES, minionCheckedFiles(minion));
        this.minion = minion;
    }

    /**
     * If this method is invoked from a context where
     * {@link com.addthis.hydra.job.Minion#doStart()}
     * is above it on the stack frame then minion.shutdown
     * has been set to true and therefore do not call
     * System.exit() or the shutdown hook will deadlock.
     */
    @Override
    public void onFailure() {
        log.warn("MinionWriteableDiskCheck failed, setting diskReadOnly=true and shutting down");
        this.minion.setDiskReadOnly(true);
        this.minion.sendHostStatus();
        if (!this.minion.getShutdown()) {
            System.exit(1);
        }
    }

    public final HealthCheckThread startHealthCheckThread() {
        return this.startHealthCheckThread(MINION_DISK_CHECK_INTERVAL, "minionDiskCheck");
    }

    private static List<File> minionCheckedFiles(Minion minion) {
        List<File> checked_files = new ArrayList<>();
        checked_files.add(minion.getRootDir());
        return checked_files;
    }
}
