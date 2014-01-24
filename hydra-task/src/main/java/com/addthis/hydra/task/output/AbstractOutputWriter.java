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
package com.addthis.hydra.task.output;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.JitterClock;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.filter.bundle.BundleFilter;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * The OutputWriter has a single concurrent queue onto which bundles
 * are placed in preparation for moving these bundles to disk. One
 * or more disk flushing threads are responsible for moving bundles
 * from the central queue to the disk.
 * <p/>
 * If the disk flushing threads fall behind the threads producing bundles,
 * then the central concurrent queue can grow too large. There are two
 * strategies for shrinking the concurrent queue when it grows too large.
 * In one strategy a thread that is producing bundles will assist in
 * moving bundles from the central queue to the disk. In the second strategy
 * the thread that is producing bundles will wait for the disk flushing
 * threads to shrink the queue.
 * <p/>
 * The default behavior is the first strategy. The second strategy is desirable
 * when you want to ensure that bundles are emitted to the destination sink
 * in the order in which the bundles are produced. The second strategy is enabled
 * by setting {@link #waitForDiskFlushThread} to true.
 */
public abstract class AbstractOutputWriter implements Codec.SuperCodable {

    private static Logger log = LoggerFactory.getLogger(AbstractOutputWriter.class);

    private static final int maxBufferSize = (int) Math.pow(10.0, 8.0);

    /**
     * Options for data layout within the output files.
     * This field is required.
     */
    @Codec.Set(codable = true, required = true)
    protected OutputStreamFormatter format;

    /**
     * Maximum number of bundles that can be stored
     * in the bundle cache before the asynchronous
     * flush is invoked. Default is 100.
     */
    @Codec.Set(codable = true)
    private int maxBundles = 100;

    /**
     * The total cache size is equal to
     * maxBundles * bufferSizeRatio. bufferSizeRatio
     * must be greater than 1. Default value is 100.
     */
    @Codec.Set(codable = true)
    private int bufferSizeRatio = 100;

    /**
     * If true then wait until the disk flush
     * threads have caught up when the buffer is full.
     * If false then perform a synchronous flush
     * when the buffer is full. Default is false.
     */
    @Codec.Set(codable = true)
    private boolean waitForDiskFlushThread = false;

    /**
     * Number of threads that flush data from
     * bundle cache to disk. Default is one.
     */
    @Codec.Set(codable = true)
    private int diskFlushThreads = 1;

    @Codec.Set(codable = true)
    private BundleFilter filter;

    private final Semaphore diskFlushThreadSemaphore = new Semaphore(0);

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private DiskFlushThread[] diskFlushThreadArray;
    protected ScheduledExecutorService writerMaintenanceThread =
            MoreExecutors.getExitingScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(1,
                            new ThreadFactoryBuilder().setNameFormat("AbstractOutputWriterCleanUpThread-%d").build()));
    private QueueWriter queueWriter;
    private volatile AtomicBoolean exiting = new AtomicBoolean(false);

    public final void writeLine(String file, Bundle nextLine) {
        if (stopped.get()) {
            log.warn("Tried to write a line after the writer has been stopped, line was: " + nextLine);
            throw new RuntimeException("Tried to write a line after the writer has been stopped");
        }
        if (filter == null || filter.filter(nextLine)) {
            queueWriter.addBundle(file, nextLine);
        }
    }

    protected abstract void doCloseOpenOutputs();

    public final void closeOpenOutputs() {
        try {
            exiting.set(true);
            // first stop the async flush threads
            shutdownMaintenanceThreads();
            shutdownDiskFlushThreads();
            queueWriter.drain(true);
            doCloseOpenOutputs();
        } finally {
            stopped.set(true);
        }
    }

    private boolean bufferSizeInRange(int bufferSize) {
        return bufferSize > maxBundles && bufferSize < maxBufferSize;
    }

    @Override
    public void postDecode() {
        /**
         * The next several lines of logic are to handle
         * ridiculous input values for maxBundles and bufferSizeRatio.
         */
        int bufferSize = maxBundles * bufferSizeRatio;
        if (!bufferSizeInRange(bufferSize)) {
            bufferSize = maxBundles * 10;
        }
        if (!bufferSizeInRange(bufferSize)) {
            bufferSize = maxBundles * 2;
        }
        if (!bufferSizeInRange(bufferSize)) {
            bufferSize = maxBundles + 1;
        }

        queueWriter = new QueueWriter(bufferSize);

        // thread to force drain queues that have data but haven't reached their drain threshold
        if (!waitForDiskFlushThread) {
            writerMaintenanceThread.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    long deltaThreshold = (JitterClock.globalTime() - 60000);
                    if (queueWriter.size() > 0 && queueWriter.lastUpdatedTime < deltaThreshold) {
                        queueWriter.drain(false);
                    }
                }
            }, 10000, 10000, TimeUnit.MILLISECONDS);
        }

        diskFlushThreadArray = new DiskFlushThread[diskFlushThreads];
        for (int i = 0; i < diskFlushThreads; i++) {
            diskFlushThreadArray[i] = new DiskFlushThread(i);
            diskFlushThreadArray[i].setDaemon(true);
            diskFlushThreadArray[i].start();
        }
    }

    @Override
    public void preEncode() {
        // nothing to do here
    }

    private class QueueWriter {

        final BlockingQueue<WriteTuple> buffer;
        volatile long lastUpdatedTime = JitterClock.globalTime();

        private QueueWriter(int bufferSize) {
            buffer = new ArrayBlockingQueue<>(bufferSize);
        }

        public int size() {
            return buffer.size();
        }

        public List<WriteTuple> drainOutputBundles(List<WriteTuple> outputList, int maxElements) {
            buffer.drainTo(outputList, maxElements);
            return outputList;
        }

        public List<WriteTuple> drainOutputBundles(int maxElements) {
            List<WriteTuple> outputList = new ArrayList<>(maxElements);
            buffer.drainTo(outputList, maxElements);
            return outputList;
        }

        /**
         * Helper function to {@link #addBundle} method. Returns
         * when no further processing is needed on the input
         * tuple. Method returns when the bundle is successfully
         * inserted into the buffer or when an exception is thrown.
         */
        private void addBundleHelper(WriteTuple tuple) {
            boolean tupleProcessed = false;

            while (!tupleProcessed) {
                try {
                    if (waitForDiskFlushThread) {
                        buffer.put(tuple);
                        tupleProcessed = true;
                    } else {
                        tupleProcessed = buffer.offer(tuple);
                    }
                } catch (InterruptedException e) {
                    log.warn("error writing to buffer: " + e, e);
                    tupleProcessed = true;
                }
                if (!tupleProcessed) {
                    try {
                        List<WriteTuple> outputList = drainOutputBundles(maxBundles);
                        dequeueWrite(outputList);
                    } catch (IOException e) {
                        log.warn("error dequeuing write: " + e, e);
                        tupleProcessed = true;
                    }
                }
            }
        }

        public void addBundle(String file, Bundle nextLine) {
            WriteTuple tuple = new WriteTuple(file, nextLine);

            addBundleHelper(tuple);

            lastUpdatedTime = JitterClock.globalTime();

            /**
             * Avoid sending an avalanche of flush messages
             * to the disk flushing threads by sending a
             * message when the disk flush threads are waiting.
             */
            if (buffer.size() > maxBundles && diskFlushThreadSemaphore.availablePermits() == 0) {
                diskFlushThreadSemaphore.release(diskFlushThreads);
            }
        }

        public void drain(boolean iterate) {
            do {
                try {
                    List<WriteTuple> outputList = drainOutputBundles(size());
                    dequeueWrite(outputList);
                } catch (IOException e) {
                    log.warn("error draining queue: " + e, e);
                    }
            }
            while (iterate && size() > 0);
        }
    }

    protected final class WriteTuple {

        public final String fileName;
        public final Bundle bundle;

        WriteTuple(String fileName, Bundle bundle) {
            this.fileName = fileName;
            this.bundle = bundle;
        }
    }

    protected final class DiskFlushThread extends Thread {

        final List<WriteTuple> outputList;

        DiskFlushThread(int id) {
            super("OutputWriterDiskFlushThread-" + id);
            outputList = new ArrayList<>(maxBundles);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    diskFlushThreadSemaphore.acquireUninterruptibly();

                    int outstandingBundles;
                    do {
                        if (exiting.get()) {
                            return;
                        }

                        /**
                         * Do not drain the entire output queue in order
                         * to allow other DiskFlushThreads to concurrently
                         * transfer bundles from the queue to the disk.
                         */
                        outputList.clear();
                        queueWriter.drainOutputBundles(outputList, maxBundles);
                        dequeueWrite(outputList);
                        outstandingBundles = queueWriter.size();
                    }
                    while (outstandingBundles > maxBundles);
                } catch (Exception ex) {
                    log.warn("output writer disk flush error : " + ex, ex);
                    }
            }
        }

    }

    /**
     * called by multiple thread consumers of the input queue. must be thread
     * safe.
     */
    protected abstract boolean dequeueWrite(List<WriteTuple> outputTuples) throws IOException;

    private void shutdownMaintenanceThreads() {
        writerMaintenanceThread.shutdown();
        try {
            if (!writerMaintenanceThread.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Waited 30 seconds for write maintenance termination but it did not finish");
            }
        } catch (InterruptedException ie) {
            log.warn("Thread interrupted while wating for write maintenance termination");
        }
    }

    private void shutdownDiskFlushThreads() {
        diskFlushThreadSemaphore.release(diskFlushThreads);

        for (int i = 0; i < diskFlushThreads; i++) {
            try {
                diskFlushThreadArray[i].join();
            } catch (InterruptedException ex) {
                log.warn("shutdown disk flush threads error : " + ex, ex);
                }
        }
    }

    public final AbstractOutputWriter setFormat(OutputStreamFormatter format) {
        this.format = format;
        return this;
    }

}
