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

import java.io.IOException;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.text.DecimalFormat;

import com.addthis.basis.util.JitterClock;
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
public final class TaskFeeder extends Thread {

    private static final Logger log = LoggerFactory.getLogger(TaskFeeder.class);
    private static final Bundle TERM_BUNDLE = new KVBundle();
    private static final boolean exit = Parameter.boolValue("task.exit", true);
    private static final long maxRead = Parameter.longValue("task.read.max", 0);
    private static final long maxProcess = Parameter.longValue("task.proc.max", 0);
    private static final long oomAfter = Parameter.longValue("task.oom.after", 0);
    private static final int oomAlloc = Parameter.intValue("task.oom.alloc", 8192);
    private static final DecimalFormat timeFormat = new DecimalFormat("#,###.00");
    private static final DecimalFormat countFormat = new DecimalFormat("#,###");
    private static final int QUEUE_DEPTH = Parameter.intValue("task.queue.depth", 100);
    private static final int stealThreshold = Parameter.intValue("task.queue.worksteal.threshold", 50);
    private static final boolean shouldSteal = Parameter.boolValue("task.worksteal", false);
    private static final boolean interruptOnExit = Parameter.boolValue("task.exit.interrupt", false);

    private boolean exiting = !Parameter.boolValue("task.feed", true);
    private AtomicBoolean terminated = new AtomicBoolean(false);
    private long start = System.currentTimeMillis();
    private BundleField shardField;

    private TaskDataSource source;
    private final TaskRunTarget task;
    private final int readers;

    private final Thread threads[];
    private final BlockingQueue<Bundle> queues[];
    private volatile boolean queuesInit;
    private final AtomicBoolean errored;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger processed = new AtomicInteger(0);
    private final AtomicLong totalReads = new AtomicLong(0);
    private final AtomicLong currentStreamReads = new AtomicLong(0);
    private final AtomicInteger threadsRunning = new AtomicInteger(0);

    private final Meter stealAttemptMeter;
    private final Meter stealSuccessMeter;
    private final Histogram modHistrogram;

    public TaskFeeder(TaskRunTarget task, int feeders) {
        super("SourceReader");

        stealAttemptMeter = Metrics.newMeter(getClass(), "stealAttemptRate", "steals", TimeUnit.SECONDS);
        stealSuccessMeter = Metrics.newMeter(getClass(), "stealSuccessRate", "steals", TimeUnit.SECONDS);
        modHistrogram = Metrics.newHistogram(getClass(), "mod");

        this.source = task.getSource();
        this.errored = task.getErrored();
        this.task = task;
        this.readers = feeders;

        log.info("starting " + feeders + " thread(s) for src=" + source);

        shardField = source.getShardField();
        threads = new Thread[feeders];
        queues = new LinkedBlockingQueue[feeders];

        threadsRunning.set(threads.length);
        for (int i = 0; i < threads.length; i++) {
            final int processorID = i;
            queues[i] = new LinkedBlockingQueue<>(QUEUE_DEPTH);
            threads[i] = new Thread(this, "MapProcessor #" + i) {
                @Override
                public void run() {
                    mapperRun(processorID);
                }
            };
            threads[i].start();
        }
        queuesInit = true;
        start();

        if (oomAfter > 0) {
            startOomThread();
        }
    }

    private void mapperRun(int worker) {
        while (true) {
            try {
                Bundle next = popQueue(worker);
                if (next == null) {
                    return;
                }
                task.process(next);
                long proctotal = processed.incrementAndGet();
                /* optional cap on processing */
                if (maxProcess > 0 && proctotal >= maxProcess) {
                    terminate();
                    return;
                }
            } catch (InterruptedException e) {
                return;
            } catch (Exception ex) {
                errored.set(true);
                log.error("", ex);
            }
        }
    }

    private void pushQueue(int queueNum, Bundle item) throws InterruptedException {
        BlockingQueue<Bundle> queue = queues[queueNum];
        queue.put(item);
    }

    private Bundle popQueue(int queueNum) throws InterruptedException {
        BlockingQueue<Bundle> queue = queues[queueNum];
        Bundle item = null;
        if (shouldSteal) {
            item = queue.poll();
            if (item == null) {
                stealAttemptMeter.mark();
                item = steal(queueNum);
                if (item != null) {
                    stealSuccessMeter.mark();
                }
            }
        }
        if (item == null) {
            item = queue.take();
        }

        return item == TERM_BUNDLE ? null : item;
    }

    private Bundle steal(int queueNum) throws InterruptedException {
        if (!queuesInit) {
            return null;
        }
        int stealQ = 0;
        Bundle item = null;
        for (int i = 0; i < queues.length; i++) {
            if (queues[i].size() >= stealThreshold) {
                item = queues[stealQ].poll();
                if (item != null) {
                    break;
                }
            }
        }
        if (item == TERM_BUNDLE) {
            queues[stealQ].put(item);
            return null;
        }
        return item;
    }

    public void run() {
        try {
            if (source.isEnabled()) {
                while (!errored.get() && fillBuffer()) {
                    ;
                }
            }
            if (errored.get()) {
                log.warn("fill buffer exited in error state");
            } else if (log.isDebugEnabled()) {
                log.debug("fill buffer exited");
            }
            joinProcessors();
            log.info("exit " + threads.length + " threads. bundles read " + countFormat.format(totalReads) + " processed " + countFormat.format(processed));
            closeStream();
            while (!exit && !terminated.get()) {
                trySleep(1000);
            }
            task.taskComplete();
            // critical to get any file meta data written before process exits
            MuxFileDirectoryCache.waitForWriteClosure();
        } catch (Throwable e) {
            errored.set(true);
            closed.set(true);
            log.warn("task feeder exception: ", e);
        }
        if (errored.get()) {
            log.warn("[" + Thread.currentThread().getName() + "] exited with error");
            if (!terminated.get()) {
                System.exit(1);
            }
        } else {
            log.info("[" + Thread.currentThread().getName() + "] exited");
        }
    }

    private void trySleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException ex) {
        }
    }

    public synchronized void terminate() {
        if (!terminated.get()) {
            log.info("terminating");
            terminated.set(true);
            exiting = true;
            if (interruptOnExit) {
                if (threads != null) {
                    for (Thread thread : threads) {
                        thread.interrupt();
                    }
                }
                this.interrupt();
            }
        }
    }

    public void waitExit() {
        // if there was an error, this exit was likely caused by this class
        // and a join would cause a deadlock
        if (!errored.get() && !closed.getAndSet(true)) {
            try {
                join();
            } catch (InterruptedException e)  {
                log.warn("", e);
            }
        }
    }

    private void joinProcessors() {
        if (log.isDebugEnabled()) log.debug("pushing terminating bundles to " + queues.length + " processors");
        for (int i = 0; i < queues.length; i++) {
            try {
                pushQueue(i, TERM_BUNDLE);
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
        if (terminated.get()) {
            long mark = JitterClock.globalTime();
            long left = 10000;
            for (int i = 0; i < threads.length; i++) {
                try {
                    threads[i].join(left);
                    left = Math.max(1, 10000 - (JitterClock.globalTime() - mark));
                } catch (InterruptedException e)  {
                    log.warn("", e);
                }
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].interrupt();
            }
        }
        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e)  {
                log.warn("", e);
            }
        }
    }

    private void closeStream() {
        if (source != null) {
            double totalTime = ((System.currentTimeMillis() - start)) / 1000.0;
            log.info(new StringBuilder().append("closing stream ")
                    .append(source.getClass().getSimpleName())
                    .append(" after ")
                    .append(timeFormat.format(totalTime)).append("s [bundles read ")
                    .append(countFormat.format(totalReads)).append(" (")
                    .append(countFormat.format((double) totalReads.get() / totalTime)).append("/s) processed ")
                    .append(countFormat.format(processed)).append(" (")
                    .append(countFormat.format((double) processed.get() / totalTime)).append("/s) ]")
                    .toString());
            source.close();
            source = null;
            currentStreamReads.set(0);
        }
    }

    private boolean fillBuffer() throws IOException {
        // iterate over inputs and execute default target
        if (exiting) {
            closeStream();
            return false;
        }
        try {
            Bundle p = source.next();
            if (p == null) {
                log.warn("stream " + source + " returned null packet");
                return false;
            }
            totalReads.incrementAndGet();
            if (maxRead > 0 && totalReads.get() >= maxRead) {
                terminate();
            }
            currentStreamReads.incrementAndGet();
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
        } catch (NoSuchElementException ex) {
            exiting = true;
            log.warn("exiting on premature stream termination");
        } catch (InterruptedException interruptedException) {
            exiting = true;
        } catch (Exception ex) {
            log.warn("Exception during fillBuffer(), setting errored state and exiting: " + ex, ex);
            errored.set(true);
            exiting = true;
        }
        return false;
    }

    public boolean isProcessing() {
        return true;
    }

    private void startOomThread() {
        new Thread("OOM Thread") {
            {
                setDaemon(true);
            }

            @Override
            public void run() {
                LinkedList<long[]> oomer = new LinkedList<>();
                try {
                    log.warn("[oomer] starting in " + oomAfter + " ms");
                    Thread.sleep(oomAfter);
                    log.warn("[oomer] starting. alloc=" + oomAlloc);
                    long arr[];
                    long next = 0;
                    while (true) {
                        arr = new long[oomAlloc];
                        Arrays.fill(arr, next++);
                        oomer.add(arr);
                        Thread.sleep(1);
                    }
                } catch (OutOfMemoryError ex) {
                    log.warn("[oom] success", ex);
                    } catch (Throwable ex)  {
                    log.warn("", ex);
                } finally {
                    log.warn("[oom] waiting forever");
                    while (this.isAlive()) {
                        synchronized (this) {
                            try {
                                this.wait(100);
                            } catch (InterruptedException e)  {
                                log.warn("", e);
                                return;
                            }
                        }
                    }
                }
            }
        }.start();
    }
}
