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
package com.addthis.hydra.task.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import java.text.DecimalFormat;

import com.addthis.basis.util.Parameter;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.hydra.common.hash.PluggableHashFunction;
import com.addthis.hydra.task.source.TaskDataSource;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.Uninterruptibles;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MapFeeder implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MapFeeder.class);

    private static final Bundle TERM_BUNDLE = new KVBundle();
    private static final DecimalFormat timeFormat = new DecimalFormat("#,###.00");
    private static final DecimalFormat countFormat = new DecimalFormat("#,###");
    private static final int QUEUE_DEPTH = Parameter.intValue("task.queue.depth", 100);
    private static final int stealThreshold = Parameter.intValue("task.queue.worksteal.threshold", 50);
    private static final boolean shouldSteal = Parameter.boolValue("task.worksteal", false);

    // state control
    private final AtomicBoolean errored = new AtomicBoolean(false);
    private boolean hasClosedStreams = false; // not shared with MapperTasks

    // enclosing task
    private final StreamMapper task;
    private final TaskDataSource source;

    // mapper task controls
    private final int feeders;
    private final String shardField;
    private final MapperTask[] mapperTasks;
    private final Thread[] threads;
    private final BlockingQueue<Bundle>[] queues;

    private Thread feederThread;

    // metrics
    private final long start = System.currentTimeMillis();

    @Nullable private final Meter stealAttemptMeter;
    @Nullable private final Meter stealSuccessMeter;

    public MapFeeder(StreamMapper task, TaskDataSource source, int feeders) {
        if (shouldSteal) {
            stealAttemptMeter = Metrics.newMeter(getClass(), "stealAttemptRate", "steals", TimeUnit.SECONDS);
            stealSuccessMeter = Metrics.newMeter(getClass(), "stealSuccessRate", "steals", TimeUnit.SECONDS);
        } else {
            stealAttemptMeter = null;
            stealSuccessMeter = null;
        }

        this.source = source;
        this.task = task;
        this.feeders = feeders;

        shardField = source.getShardField();
        mapperTasks = new MapperTask[feeders];
        threads = new Thread[feeders];
        queues = new LinkedBlockingQueue[feeders];

        for (int i = 0; i < threads.length; i++) {
            queues[i] = new LinkedBlockingQueue<>(QUEUE_DEPTH);
            mapperTasks[i] = new MapperTask(this, i);
            threads[i] = new Thread(mapperTasks[i], "MapProcessor #" + i);
        }
        log.info("work stealing = {}", shouldSteal);
    }

    @Override public void run() {
        feederThread = Thread.currentThread();
        log.info("starting {} thread(s) for src={}", feeders, source);
        for (Thread thread : threads) {
            thread.start();
        }

        try {
            if (source.isEnabled()) {
                while (fillBuffer()) {
                    if (task.isClosing()) {
                        closeSourceIfNeeded();
                    }
                }
            }
            closeSourceIfNeeded();
            joinProcessors();
            log.info("all ({}) task threads exited; sending taskComplete", feeders);

            // run in different threads to isolate them from interrupts. ie. "taskCompleteUninterruptibly"
            // join awaits completion, is uninterruptible, and will propogate any exception
            CompletableFuture.runAsync(task::taskComplete).join();
        } catch (Throwable t) {
            handleUncaughtThrowable(t);
        }
        log.debug("task feeder exited");
    }

    /**
     * Immediately halting is the best way we have of ensuring errors are reported without a significantly
     * more involved system.
     */
    private void handleUncaughtThrowable(Throwable t) {
        if (errored.compareAndSet(false, true)) {
            log.error("unrecoverable error in task feeder or one of its mapper threads. immediately halting jvm", t);
            Runtime.getRuntime().halt(1);
        }
    }

    private boolean fillBuffer() {
        Bundle p = nextSourceBundle();
        if (p == null) {
            return false;
        }
        int hash = p.hashCode();
        if (shardField != null) {
            String val = ValueUtil.asNativeString(p.getValue(p.getFormat().getField(shardField)));
            if (!Strings.isNullOrEmpty(val)) {
                hash = PluggableHashFunction.hash(val);
            }
        }
        int mod = Math.abs(hash % queues.length);
        try {
            queues[mod].put(p);
        } catch (InterruptedException e) {
            if (!batchFillAllDrainedQueues(mod, p)) {
                return false;
            }
        }
        return true;
    }

    private Bundle nextSourceBundle() {
        Bundle bundle = null;
        try {
            bundle = source.next();
        } catch (NoSuchElementException e) {
        }
        if (bundle == null) {
            log.info("exiting on null bundle or NoSuchElementException from {}", source);
        }
        return bundle;
    }

    private boolean batchFillAllDrainedQueues(int interruptedQueue, @Nonnull Bundle interruptedBundle) {
        boolean interruptedBundlePutBack = false;
        int fillCount;
        for (MapperTask t : mapperTasks) {
            if (t.queueDrained) {
                t.queueDrained = false;
                if (!interruptedBundlePutBack) {
                    pushQueue(t.processorID, interruptedBundle);
                    interruptedBundlePutBack = true;
                    fillCount = QUEUE_DEPTH - 1;
                } else {
                    fillCount = QUEUE_DEPTH;
                }
                if (!fillQueue(t.processorID, fillCount)) {
                    return false;
                }
            }
        }
        if (!interruptedBundlePutBack) {
            log.error("Batch fill is called when no queue is drained. This is a bug!");
            // just force the bundle into the queue that it was being put into
            pushQueue(interruptedQueue, interruptedBundle);
        }
        return true;
    }

    private boolean fillQueue(int queueNum, int count) {
        for (int i = 0; i < count; i++) {
            Bundle b = nextSourceBundle();
            if (b == null) {
                return false;
            }
            pushQueue(queueNum, b);
        }
        return true;
    }

    private void pushQueue(int queueNum, Bundle item) {
        BlockingQueue<Bundle> queue = queues[queueNum];
        Uninterruptibles.putUninterruptibly(queue, item);
    }

    private void joinProcessors() {
        log.debug("pushing terminating bundles to {} processors", queues.length);
        for (int i = 0; i < queues.length; i++) {
            pushQueue(i, TERM_BUNDLE);
        }
        for (Thread thread : threads) {
            Uninterruptibles.joinUninterruptibly(thread);
        }
    }

    private void closeSourceIfNeeded() {
        if (!hasClosedStreams) {
            hasClosedStreams = true;
            log.info("closing stream {}", source.getClass().getSimpleName());
            // TODO: better API/ force sources to behave more sensibly
            CompletableFuture.runAsync(source::close).join();
        }
    }

    private void signalDrainedMapperTaskQueue(int mapperTaskId) {
        log.debug("MapperTask #{} drained its queue", mapperTaskId);
        feederThread.interrupt();
    }

    private static class MapperTask implements Runnable {
        private final int processorID;
        private final MapFeeder mapFeeder;
        private final Meter bundleProcMeter;
        private final Meter queueDrainMeter;
        private final Meter feederInterruptMeter;

        private long bundlesConsumedSinceLastDrainSignal;
        private volatile boolean queueDrained;

        public MapperTask(MapFeeder mapFeeder, int processorID) {
            this.processorID = processorID;
            this.mapFeeder = mapFeeder;
            bundleProcMeter = Metrics.newMeter(getClass(), "bundleProcRate." + processorID, "bundles", TimeUnit.SECONDS);
            queueDrainMeter = Metrics.newMeter(getClass(), "queueDrainRate." + processorID, "dranins", TimeUnit.SECONDS);
            feederInterruptMeter = Metrics.newMeter(getClass(), "feederInterrupt." + processorID, "interrupts", TimeUnit.SECONDS);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Bundle next = popQueue();
                    if (next == null) {
                        printMeter("bundles", bundleProcMeter);
                        printMeter("drains", queueDrainMeter);
                        printMeter("interrupts", feederInterruptMeter);
                        return;
                    }
                    bundlesConsumedSinceLastDrainSignal++;
                    mapFeeder.task.process(next);
                    bundleProcMeter.mark();
                } catch (Throwable t) {
                    mapFeeder.handleUncaughtThrowable(t);
                }
            }
        }

        private void printMeter(String name, Meter meter) {
            log.info("Meter {}:", name);
            log.info("             count = {}", meter.count());
            log.info("         mean rate = {}", meter.meanRate());
            log.info("     1-minute rate = {}", meter.oneMinuteRate());
            log.info("     5-minute rate = {}", meter.fiveMinuteRate());
            log.info("    15-minute rate = {}", meter.fifteenMinuteRate());
        }

        @Nullable private Bundle popQueue() throws InterruptedException {
            BlockingQueue<Bundle> queue = mapFeeder.queues[processorID];
            Bundle item = null;
            if (shouldSteal) {
                // first check our own queue
                item = queue.poll();
                if (item == null) {
                    queueDrained = true;
                    queueDrainMeter.mark();
                    // if source is slower than processing, this could avoid interrupting mapFeeder too much - because
                    // we know QUEUE_DEPTH bundles will be batch filled, no need to ask again until at least that many
                    // have been processed. though if source is the bottleneck, ideally we would do none of this - the
                    // interruptions, even at a lower frequency, still add to mapFeeder's overhead.
                    if (bundlesConsumedSinceLastDrainSignal >= QUEUE_DEPTH) {
                        mapFeeder.signalDrainedMapperTaskQueue(processorID);
                        bundlesConsumedSinceLastDrainSignal = 0;
                        feederInterruptMeter.mark();
                    }
                }
            }
            if (item == null) {
                item = queue.take();
            }
            if (item == TERM_BUNDLE) {
                return null;
            } else {
                return item;
            }
        }

        @Nullable private Bundle steal(BlockingQueue<Bundle> primaryQueue) throws InterruptedException {
            mapFeeder.stealAttemptMeter.mark();
            for (BlockingQueue<Bundle> queue : mapFeeder.queues) {
                if ((queue != primaryQueue) && (queue.size() >= stealThreshold)) {
                    Bundle item = queue.poll();
                    if (item == TERM_BUNDLE) {
                        queue.put(item);
                    } else if (item != null) {
                        mapFeeder.stealSuccessMeter.mark();
                        return item;
                    }
                }
            }
            return null;
        }
    }
}
