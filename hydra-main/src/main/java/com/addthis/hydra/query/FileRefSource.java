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
package com.addthis.hydra.query;

import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

import com.addthis.basis.util.Parameter;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

class FileRefSource extends FileSource {
    static final long maxGetFileReferencesTime = Parameter.intValue("qmaster.maxListFilesTime", 60);

    @GuardedBy("lock")
    private final Multimap<Integer, FileReference> fileRefMap;
    private final CountDownLatch gate;
    private final StampedLock lock;

    @GuardedBy("lock")
    private boolean shortCircuited;

    public FileRefSource(ChannelMaster master) {
        super(master);
        this.lock = new StampedLock();
        this.shortCircuited = false;
        this.fileRefMap = HashMultimap.create();
        this.gate = new CountDownLatch(1);
    }

    @Override public void receiveReference(FileReference ref) {
        String[] tokens = ref.name.split("/");
        Integer id = Integer.parseInt(tokens[3]);
        long stamp = lock.tryWriteLock();
        if (stamp == 0) {
            // assume shortCircuited would be set after waiting anyway
            return;
        }
        try {
            if (!shortCircuited) {
                fileRefMap.put(id, ref);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override public void receiveComplete() throws Exception {
        gate.countDown();
    }

    public Multimap<Integer, FileReference> getWithShortCircuit() throws InterruptedException {
        if (!gate.await(maxGetFileReferencesTime, TimeUnit.SECONDS)) {
            log.warn("Timed out waiting for mesh file list");
        }
        long stamp = lock.writeLock();
        try {
            shortCircuited = true;
            return fileRefMap;
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}
