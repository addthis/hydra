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

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

import com.addthis.basis.util.Parameter;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;

import com.google.common.base.Joiner;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import io.netty.util.internal.PlatformDependent;

/**
 * Supports multi-producer (server-to-server) file ref collection in a lock-free manner. This class is only
 * thread-safe for the meshy-facing components though, and should not be concurrently accessed from user code.
 *
 * It uses a lock-free multi-producer/ single-consumer queue implementation from netty with some extra logic to
 * support blocking/timed polls (the netty version only implements {@link Queue} and not {@link BlockingQueue}).
 * The implementation of the blocking strives to avoid random sleeping or spin-blocking by using a {@link StampedLock}
 * as a sort of binary semaphore. This gets us the park/unpark semantics we want without having to either directly
 * implement that logic or throw away the spin-then-park loop optimizations that doug lee et al already benchmarked.
 *
 * There may be a "better" way to accomplish this than to use a {@link StampedLock}, but its code seems to suggest
 * that it does a pretty efficient job at handling the methods we expect to call at their respective frequencies.
 */
class FileRefSource extends FileSource {
    private static final long MAX_WAIT_SECONDS = Parameter.intValue("qmaster.maxListFilesTime", 60);
    private static final long MAX_WAIT_NANOS = TimeUnit.SECONDS.toNanos(MAX_WAIT_SECONDS);
    private static final FileReference END_SIGNAL = new FileReference("END_SIGNAL", 0, 0);

    /** Safe for multiple producers, but only a single consumer (lock-free). Borrowed from netty. */
    private final Queue<FileReference> references;
    /** Only used to park the consumer thread while waiting for more references (lock-free). */
    private final StampedLock semaphore;

    private volatile boolean shortCircuited;
    // uses shortCircuited to ensure visibility after lazy instantiation, but otherwise is thread-safe on its own
    @GuardedBy("shortCircuited") private ConcurrentHashMultiset<String> lateFileRefFinds;

    FileRefSource(ChannelMaster master) {
        super(master);
        this.references = PlatformDependent.newMpscQueue();
        this.semaphore = new StampedLock();
        // throw away a writeLock so that we can immediately park the consumer
        this.semaphore.writeLock();
        this.shortCircuited = false;
    }

    /** Called for every received file reference. Note! This method may be called concurrently by many threads. */
    @Override public void receiveReference(FileReference ref) {
        if (!shortCircuited) {
            references.add(ref);
            semaphore.tryUnlockWrite();
        } else {
            lateFileRefFinds.add(ref.getHostUUID());
            log.debug("throwing away ref due to short circuit {}", ref);
        }
    }

    /** Called once after every call to receiveReference has finished. This method is called by one of many threads. */
    @Override public void receiveComplete() {
        references.add(END_SIGNAL);
        semaphore.tryUnlockWrite();
        log.debug("query file ref source - receive complete for {}", this);
        if (shortCircuited) {
            log.warn("Late File Finds for {}:\n{}", this, Joiner.on('\n').join(lateFileRefFinds.entrySet()));
        }
    }

    public Multimap<Integer, FileReference> getWithShortCircuit() throws InterruptedException {
        Multimap<Integer, FileReference> fileRefMap = HashMultimap.create();
        long end = System.nanoTime() + MAX_WAIT_NANOS;
        long remaining = MAX_WAIT_NANOS;
        while (true) {
            if (semaphore.tryWriteLock(remaining, TimeUnit.NANOSECONDS) == 0) {
                lateFileRefFinds = ConcurrentHashMultiset.create();
                shortCircuited = true;
                log.warn("Timed out waiting for mesh file list: {}. After waiting {}", this, MAX_WAIT_NANOS);
                return fileRefMap;
            }
            FileReference fileReference = references.poll();
            while (fileReference != null) {
                if (fileReference == END_SIGNAL) {
                    log.debug("Finished collecting file refs for: {}", this);
                    return fileRefMap;
                }
                fileRefMap.put(getTaskId(fileReference), fileReference);
                fileReference = references.poll();
            }
            remaining = end - System.nanoTime();
        }
    }

    private static int getTaskId(FileReference fileReference) {
        String[] tokens = fileReference.name.split("/");
        return Integer.parseInt(tokens[3]);
    }
}
