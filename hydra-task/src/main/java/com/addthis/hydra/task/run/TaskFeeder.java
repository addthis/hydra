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
package com.addthis.hydra.task.run;

import javax.annotation.Nullable;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.text.DecimalFormat;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.hydra.common.hash.PluggableHashFunction;
import com.addthis.hydra.task.source.TaskDataSource;
import com.addthis.muxy.MuxFileDirectoryCache;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * reads TaskDataSource list defined in TreeMapJob (config) using a
 * defined set of processor threads that push bundles to TreeMapper.
 */
public final class TaskFeeder implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TaskFeeder.class);
    private static final Bundle TERM_BUNDLE = new KVBundle();
    private static final DecimalFormat timeFormat = new DecimalFormat("#,###.00");
    private static final DecimalFormat countFormat = new DecimalFormat("#,###");
    private static final int QUEUE_DEPTH = Parameter.intValue("task.queue.depth", 100);
    private static final int stealThreshold = Parameter.intValue("task.queue.worksteal.threshold", 50);
    private static final boolean shouldSteal = Parameter.boolValue("task.worksteal", false);

    // state control
    private final AtomicBoolean errored = new AtomicBoolean(false);

    private final long start = System.currentTimeMillis();
    private final BundleField shardField;

    private final TaskDataSource source;
    private final TaskRunTarget task;
    private final int feeders;

    private final Thread[] threads;
    private final BlockingQueue<Bundle>[] queues;
    private final AtomicInteger processed = new AtomicInteger(0);
    private final AtomicLong totalReads = new AtomicLong(0);

    private final Meter stealAttemptMeter;
    private final Meter stealSuccessMeter;
    private final Histogram modHistrogram;

    public TaskFeeder(TaskRunTarget task, int feeders) {
        stealAttemptMeter = Metrics.newMeter(getClass(), "stealAttemptRate", "steals", TimeUnit.SECONDS);
        stealSuccessMeter = Metrics.newMeter(getClass(), "stealSuccessRate", "steals", TimeUnit.SECONDS);
        modHistrogram = Metrics.newHistogram(getClass(), "mod");

        this.source = task.getSource();
        this.task = task;
        this.feeders = feeders;

        shardField = source.getShardField();
        threads = new Thread[feeders];
        queues = new LinkedBlockingQueue[feeders];

        for (int i = 0; i < threads.length; i++) {
            queues[i] = new LinkedBlockingQueue<>(QUEUE_DEPTH);
            threads[i] = new Thread(new MapperTask(this, i), "MapProcessor #" + i);
        }
    }

    @Override public void run() {
        log.info("starting {} thread(s) for src={}", feeders, source);
        for (Thread thread : threads) {
            thread.start();
        }

        try {
            if (source.isEnabled()) {
                while (fillBuffer()) {
                }
            }
            closeStream();
            joinProcessors();
            log.info("exit {} threads. bundles read {} processed {}",
                     feeders, countFormat.format(totalReads), countFormat.format(processed));
            task.taskComplete();
            // critical to get any file meta data written before process exits
            MuxFileDirectoryCache.waitForWriteClosure();
        } catch (Throwable t) {
            handleUncaughtThrowable(t);
        }
        log.info("[{}] exited", Thread.currentThread().getName());
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
        // iterate over inputs and execute default target
        try {
            Bundle p = source.next();
            if (p == null) {
                log.info("exiting on null bundle from {}", source);
                return false;
            }
            totalReads.incrementAndGet();
            int hash = p.hashCode();
            if (shardField != null) {
                String val = ValueUtil.asNativeString(p.getValue(shardField));
                if (!Strings.isEmpty(val)) {
                    hash = PluggableHashFunction.hash(val);
                }
            }
            int mod = Math.abs(hash % queues.length);
            modHistrogram.update(mod);
            pushQueue(mod, p);
            return true;
        } catch (NoSuchElementException ignored) {
            log.info("exiting on premature stream termination");
        } catch (InterruptedException ignored) {
            log.info("exiting on interrupt (probably max runtime)");
        }
        return false;
    }

    private void pushQueue(int queueNum, Bundle item) throws InterruptedException {
        BlockingQueue<Bundle> queue = queues[queueNum];
        queue.put(item);
    }

    private void joinProcessors() {
        log.debug("pushing terminating bundles to {} processors", queues.length);
        for (int i = 0; i < queues.length; i++) {
            try {
                pushQueue(i, TERM_BUNDLE);
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                log.warn("", e);
            }
        }
    }

    private void closeStream() {
        double totalTime = (System.currentTimeMillis() - start) / 1000.0;
        log.info(new StringBuilder().append("closing stream ")
                                    .append(source.getClass().getSimpleName())
                                    .append(" after ")
                                    .append(timeFormat.format(totalTime))
                                    .append("s [bundles read ")
                                    .append(countFormat.format(totalReads))
                                    .append(" (")
                                    .append(countFormat.format((double) totalReads.get() / totalTime))
                                    .append("/s) processed ")
                                    .append(countFormat.format(processed))
                                    .append(" (")
                                    .append(countFormat.format((double) processed.get() / totalTime))
                                    .append("/s) ]")
                                    .toString());
        source.close();
    }

    private static class MapperTask implements Runnable {
        private final int processorID;
        private final TaskFeeder taskFeeder;

        public MapperTask(TaskFeeder taskFeeder, int processorID) {
            this.processorID = processorID;
            this.taskFeeder = taskFeeder;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Bundle next = popQueue();
                    if (next == null) {
                        return;
                    }
                    taskFeeder.task.process(next);
                    taskFeeder.processed.incrementAndGet();
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    taskFeeder.handleUncaughtThrowable(t);
                }
            }
        }

        @Nullable private Bundle popQueue() throws InterruptedException {
            BlockingQueue<Bundle> queue = taskFeeder.queues[processorID];
            Bundle item = null;
            if (shouldSteal) {
                item = queue.poll();
                if (item == null) {
                    taskFeeder.stealAttemptMeter.mark();
                    item = steal();
                    if (item != null) {
                        taskFeeder.stealSuccessMeter.mark();
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

        @Nullable private Bundle steal() throws InterruptedException {
            int stealQ = 0;
            Bundle item = null;
            for (BlockingQueue<Bundle> queue : taskFeeder.queues) {
                if (queue.size() >= stealThreshold) {
                    item = taskFeeder.queues[stealQ].poll();
                    if (item != null) {
                        break;
                    }
                }
            }
            if (item == TERM_BUNDLE) {
                taskFeeder.queues[stealQ].put(item);
                return null;
            }
            return item;
        }
    }
}
