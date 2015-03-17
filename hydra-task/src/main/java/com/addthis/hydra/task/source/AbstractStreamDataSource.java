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
package com.addthis.hydra.task.source;

import javax.annotation.Nullable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.codec.annotations.Time;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;

/**
 * Abstract implementation of TaskDataSource
 * <p/>
 * There are many common features that streaming data source needs to deal with
 * and this class performs those tasks so that subclasses can leverage the common
 * functionality.
 * <p/>
 */
public abstract class AbstractStreamDataSource extends TaskDataSource implements BundleFactory {

    private static final Logger log = LoggerFactory.getLogger(AbstractStreamDataSource.class);

    /**
     * Maximum size of the queue that stores bundles prior to their processing.
     * Default is either "dataSourceMeshy2.buffer" configuration value or 128.
     */
    @JsonProperty(required = true) protected int buffer;

    @Time(TimeUnit.MILLISECONDS)
    @JsonProperty protected int pollInterval;

    @JsonProperty protected int pollCountdown;

    /** Enable metrics visible only from jmx */
    @JsonProperty protected boolean jmxMetrics;

    /* metrics */
    protected final Histogram fileSizeHisto = Metrics.newHistogram(getClass(), "fileSizeHisto");
    protected final Timer readTimer = Metrics.newTimer(getClass(), "readTimer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    protected final Counter openNew = Metrics.newCounter(getClass(), "openNew");
    protected final Counter openIndex = Metrics.newCounter(getClass(), "openIndex");
    protected final Counter openSkip = Metrics.newCounter(getClass(), "openSkip");
    protected final Counter skipping = Metrics.newCounter(getClass(), "skipping");
    protected final Counter reading = Metrics.newCounter(getClass(), "reading");
    protected final Counter opening = Metrics.newCounter(getClass(), "opening");
    protected final Counter globalBundleSkip = Metrics.newCounter(getClass(), "globalBundleSkip");

    // State control
    protected final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    protected final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    protected final CountDownLatch initialized = new CountDownLatch(1);
    protected boolean localInitialized = false;

    protected BlockingQueue<Bundle> queue;

    @Override public void init() {
        queue = new LinkedBlockingQueue<>(buffer);
    }

    @Nullable @Override public Bundle next() throws DataChannelError {
        int countdown = pollCountdown;
        while (((localInitialized || waitForInitialized()) && (pollCountdown == 0)) || (countdown-- > 0)) {
            long startTime = jmxMetrics ? System.currentTimeMillis() : 0;
            Bundle next = pollAndCloseOnInterrupt(pollInterval, TimeUnit.MILLISECONDS);
            if (jmxMetrics) {
                readTimer.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            }
            if (next != null) {
                return next;
            }
            if (closeFuture.isDone()) {
                closeFuture.join();
                return null;
            }
            if (pollCountdown > 0) {
                log.info("next polled null, retrying {} more times. shuttingDown={}", countdown, shuttingDown.get());
            }
            log.info(fileStatsToString("null poll "));
        }
        if (countdown < 0) {
            log.info("exit with no data during poll countdown");
        }
        return null;
    }

    private Bundle pollAndCloseOnInterrupt(long pollFor, TimeUnit unit) {
        boolean interrupted = false;
        try {
            long remainingNanos = unit.toNanos(pollFor);
            long end = System.nanoTime() + remainingNanos;
            while (true) {
                try {
                    return queue.poll(remainingNanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                    log.info("interrupted while polling for bundles; closing source then resuming poll");
                    close();
                    remainingNanos = end - System.nanoTime();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override public String toString() {
        return populateToString(Objects.toStringHelper(this));
    }

    protected String fileStatsToString(String reason) {
        return populateToString(Objects.toStringHelper(reason));
    }

    private String populateToString(Objects.ToStringHelper helper) {
        return helper.add("reading", reading.count())
                     .add("opening", opening.count())
                     .add("unseen", openNew.count())
                     .add("continued", openIndex.count())
                     .add("skipping", skipping.count())
                     .add("skipped", openSkip.count())
                     .add("bundles-skipped", globalBundleSkip.count())
                     .add("median-size", fileSizeHisto.getSnapshot().getMedian())
                     .toString();
    }

    @Nullable @Override public Bundle peek() throws DataChannelError {
        if (localInitialized || waitForInitialized()) {
            try {
                return queue.peek();
            } catch (Exception ex) {
                throw propagate(ex);
            }
        }
        return null;
    }

    private boolean waitForInitialized() {
        boolean wasInterrupted = false;
        try {
            while (!localInitialized
                   && !awaitUninterruptibly(initialized, 3, TimeUnit.SECONDS)
                   && !shuttingDown.get()) {
                log.info(fileStatsToString("waiting for initialization"));
                if (Thread.interrupted()) {
                    wasInterrupted = true;
                    log.info("interrupted while waiting for initialization; closing source then resuming wait");
                    close();
                }
            }
            log.info(fileStatsToString("initialized"));
            localInitialized = true;
            return true;
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
