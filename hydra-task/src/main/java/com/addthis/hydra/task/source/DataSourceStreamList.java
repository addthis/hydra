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

import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.basis.io.IOWrap;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.hydra.task.run.TaskRunConfig;
import com.addthis.hydra.task.stream.StreamFile;
import com.addthis.hydra.task.stream.StreamFileSource;
import com.addthis.hydra.task.stream.StreamSourceHashed;
import com.addthis.hydra.task.stream.StreamSourceMeshy;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ning.compress.lzf.LZFInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import lzma.sdk.lzma.Decoder;
import lzma.streams.LzmaInputStream;

/**
 * Iterates over a source list and returns them as a continuous stream.
 */
public abstract class DataSourceStreamList extends TaskDataSource implements SuperCodable {

    private static final Logger log = LoggerFactory.getLogger(DataSourceStreamList.class);

    /**
     * Specifies conversion to bundles.
     */
    @FieldConfig(codable = true, required = true)
    protected TaskDataSource factory;

    /**
     * This field is unused.
     */
    @FieldConfig(codable = true)
    protected String injectKey = FactoryInputStream.InjectorStreamSource.DefautlInjectorKey;

    /**
     * Path to the mark directory.
     */
    @FieldConfig(codable = true)
    private String markDir = "marks";

    /**
     * Number of shards in the input source.
     */
    @FieldConfig(codable = true)
    private Integer shardTotal;

    /**
     * If specified then process only the shards specified in this array.
     */
    @FieldConfig(codable = true)
    private Integer[] shards;

    /**
     * If true then generate a hash of the filename input rather than use the {{mod}} field. Default is false.
     */
    @FieldConfig(codable = true)
    protected boolean hash;


    /**
     * If true then set hash to true when shardTotal is null or 0. Default is false.
     */
    @FieldConfig(codable = true)
    protected boolean forceHashFalse;

    /**
     * If non-null, then inject the filename into the bundle field using this field name. Default is null.
     */
    @FieldConfig(codable = true)
    protected String injectSourceName;

    @FieldConfig(codable = true)
    protected int maxCacheSize = 100;

    @FieldConfig(codable = true)
    protected int cacheFillInterval = 500;

    @FieldConfig(codable = true)
    protected int peekerThreads = 2;

    @FieldConfig(codable = true)
    protected int sourceInitThreads = 1;

    @FieldConfig(codable = true)
    protected int MAX_GET_NEXT_SOURCE_ATTEMPTS = 360000;

    @FieldConfig(codable = true)
    protected int maxReadyQueuePollAttempts = 500;

    private StreamFileSource sources;
    private SourceTracker tracker;
    private ValueObject sourceName;
    private Bundle peek;
    private ExecutorService cacheFillerService = MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(), new ThreadFactoryBuilder().setNameFormat("SourceCacheFiller-%d").build()));
    private ExecutorService sourceInitService;
    private ExecutorService peekerService;
    private Lock sourceOpenLock = new ReentrantLock();
    private volatile boolean exiting = false;
    private volatile boolean finished = false;
    private volatile boolean initialized = false;
    private SourceWrapper currentSource;
    private AtomicInteger nextWrapperId = new AtomicInteger();
    private AtomicInteger queuedSourceInitTasks = new AtomicInteger();
    private AtomicInteger peekQueue = new AtomicInteger();

    /**
     * a queue of sources that have a bundle that is ready for use
     */
    private final BlockingQueue<SourceWrapper> readyQueue = new LinkedBlockingQueue<>();

    /**
     * a list of all source wrappers we are currently tracking, used to detect exit conditions.
     */
    private final List<SourceWrapper> wrapperList = new ArrayList<>();

    /**
     * a set of sources that should be closed on exit
     */
    private final Set<SourceWrapper> closeSet = new HashSet<>();

    public abstract StreamFileSource getSourceList(Integer[] shards);

    protected DataSourceStreamList() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                exiting = true;
                finished = true;
                peekerService.shutdownNow();
                cacheFillerService.shutdownNow();
            }
        });
    }

    @Override
    public void init(TaskRunConfig config, AtomicBoolean errored) {
        try {
            doOpen(config, errored);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void doOpen(TaskRunConfig config, AtomicBoolean errored) throws Exception {
        tracker = new SourceTracker(markDir, config);
        if (shardTotal == null || shardTotal == 0) {
            shardTotal = config.nodeCount;
            if (!forceHashFalse) {
                hash = true;
            }
        }
        if (shards == null) {
            shards = config.calcShardList(shardTotal);
        }
        sources = getSourceList(shards);
        if (hash) {
            sources = new StreamSourceHashed(sources, shards, shardTotal);
        }
        cacheFillerService.execute(new CacheFiller(errored));
        log.warn("shards=[" + Strings.join(shards, ",") + " of " + shardTotal + "] sources=" + sources + " peekers=" + peekerThreads + " maxCache=" + maxCacheSize);
    }

    @Override
    public void postDecode() {
        sourceInitService = MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(sourceInitThreads, sourceInitThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(), new ThreadFactoryBuilder().setNameFormat("SourceInitThread-%d").build()));
        peekerService = MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(peekerThreads, peekerThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(), new ThreadFactoryBuilder().setNameFormat("TaskDataSourcePeeker-%d").build()));
    }

    @Override
    public void preEncode() {
        // nothing to do
    }

    @Override
    public void close() {
        shutdownAndAwaitTermination(peekerService, sourceInitService);
        exiting = true;
        for (SourceWrapper sourceWrapper : closeSet) {
            sourceWrapper.close();
        }
        tracker.close();
    }

    @Override
    public Bundle peek() {
        if (log.isDebugEnabled()) log.debug("[peek]");
        if (peek != null) {
            if (log.isDebugEnabled()) log.debug("[peek] cached " + peek);
            return peek;
        }
        if (!exiting && (initialized || waitForInitialized()) && getNextDataSource() != null) {
            currentSource.peekLock.lock();
            try {
                peek = currentSource.getSource().peek();
            } finally {
                currentSource.peekLock.unlock();
            }
            if (peek != null && sourceName != null) {
                peek.setValue(peek().getFormat().getField(injectSourceName), sourceName);
            }
            if (log.isDebugEnabled()) {
                log.debug("[peek] new peek " + peek + " readyQueue:" + readyQueue.size());
            }
            return peek;
        }
        if (log.isDebugEnabled()) {
            log.debug("nextSource was null readyQueue:" + readyQueue.size());
        }
        return null;
    }

    private boolean waitForInitialized() {
        while (!exiting) {
            if (initialized || finished) {
                break;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.warn("interrupted while waiting for initialization to be true");
                return false;
            }
        }
        return true;
    }

    private TaskDataSource getNextDataSource() {
        if (currentSource != null) {
            try {
                peekerService.execute(new Peeker(currentSource));
            } catch (RejectedExecutionException e) {
                log.warn("unable to submit new peeker, likely in shutdown mode");
            }
            currentSource = null;
        }
        SourceWrapper sourceWrapper = null;
        int attempts = 0;
        while (!exiting) {
            try {
                attempts++;
                sourceWrapper = readyQueue.poll(10, TimeUnit.MILLISECONDS);
                if (attempts > maxReadyQueuePollAttempts && finished &&
                    sourceWrapper == null &&
                    queuedSourceInitTasks.get() == 0 &&
                    readyQueue.size() == 0) {
                    // all closed
                    log.warn("source stream closed, exiting process");
                    return null;
                } else if (sourceWrapper == null && attempts > MAX_GET_NEXT_SOURCE_ATTEMPTS) {
                    log.warn("stuck in readyQueue loop queuedSourceInitTasks.get():" + queuedSourceInitTasks.get() + " finished?" + finished);
                    throw new RuntimeException("ERROR: Fail safe exiting to prevent infinite hang.  There is likely in an error above this in the logs, go look for it!");
                }
                if (attempts % 1000 == 0) {
                    log.warn("Polling Ready Queue: queuedSourceInitTasks:" + queuedSourceInitTasks.get() + " peekQueueSize:" + peekQueue.get() + " readyQueueSize:" + readyQueue.size());
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted while getting next source from readyQueue");
                return null;
            }
            // we expect that peek is already populated but lets confirm
            if (sourceWrapper != null) {
                Bundle p = null;
                sourceWrapper.peekLock.lock();
                try {
                    p = sourceWrapper.getSource().peek();
                } finally {
                    sourceWrapper.peekLock.unlock();
                }
                if (p == null) {
                    // source is empty, close it and move on
                    closeSet.remove(sourceWrapper);
                    wrapperList.remove(sourceWrapper);
                    sourceWrapper.close();
                    sourceWrapper = null;
                } else {
                    // need to keep track of partially opened sources so we can close them on exit
                    closeSet.add(sourceWrapper);
                    // we've found a good source with a peek value so we can break the search loop
                    break;
                }
            }
        }
        updateSourceMetaData(sourceWrapper);
        return sourceWrapper == null ? null : sourceWrapper.getSource();
    }

    private void updateSourceMetaData(SourceWrapper sourceWrapper) {
        if (sourceWrapper != null && (currentSource == null || sourceWrapper.getSource() != currentSource.getSource())) {
            if (injectSourceName != null) {
                if (sourceWrapper.getOstream() instanceof SourceTypeStateful) {
                    sourceName = ValueFactory.create(((SourceTypeStateful) sourceWrapper.getOstream()).getSourceIdentifier());
                } else {
                    sourceName = ValueFactory.create(sourceWrapper.getOstream().toString());
                }
            }
        }
        currentSource = sourceWrapper;
    }

    @Override
    public Bundle next() {
        if (log.isDebugEnabled()) log.debug("[next]");
        if (peek() != null) {
            Bundle next = currentSource.getSource().next();
            peek = null;
            if (log.isDebugEnabled()) log.debug("[next] " + next);
            return next;
        } else {
            return null;
        }
    }

    private void fillInputStreamCache(AtomicBoolean errored) throws InterruptedException {
        try {
            while (wrapperList.size() < maxCacheSize && queuedSourceInitTasks.get() < maxCacheSize && !finished) {
                if (exiting) {
                    log.warn("[fillCache] exiting source filler do to exiting boolean being set");
                    finished = true;
                    break;
                }
                StreamFile nextStream = sources.nextSource();
                if (nextStream == null) {
                    log.warn("[fillCache] nextStream was null, no more sources to fill. wrapped=" + wrapperList.size());
                    finished = true;
                    break;
                }
                TaskDataSource ostream = new SourceTypeStreamFile(factory.clone(), nextStream);
                if (log.isDebugEnabled()) log.debug("[fillCache] init/init stream " + nextStream);
                if (exiting) {
                    // check to make sure we aren't exiting before trying to init source
                    break;
                }
                if (!tracker.hasChanged((SourceTypeStateful) ostream)) {
                    continue;
                }

                sourceInitService.execute(new SourceInitializer(queuedSourceInitTasks.incrementAndGet(), nextStream, ostream, errored));
            }
        } catch (Exception ex) {
            log.warn("Unexpected Exception filling cacheList: " + ex.getMessage(), ex);
            exiting = true;
            throw new RuntimeException(ex);
        }
    }

    /**
     * @exclude
     */
    private class Peeker implements Runnable {

        private final SourceWrapper sourceWrapper;

        private Peeker(SourceWrapper sourceWrapper) {
            peekQueue.incrementAndGet();
            this.sourceWrapper = sourceWrapper;
        }

        @Override
        public void run() {
            if (exiting) {
                return;
            }
            sourceWrapper.peekLock.lock();
            try {
                sourceWrapper.getSource().peek();
            } finally {
                sourceWrapper.peekLock.unlock();
                // add to the ready queue
                readyQueue.add(sourceWrapper);
                peekQueue.decrementAndGet();
            }

        }
    }

    /**
     * @exclude
     */
    private class SourceInitializer implements Runnable {

        private final StreamFile streamFile;
        private final TaskDataSource source;
        private final int initId;
        private final AtomicBoolean errored;

        private SourceInitializer(int initId, StreamFile streamFile, TaskDataSource source, AtomicBoolean errored) {
            this.initId = initId;
            this.streamFile = streamFile;
            this.source = source;
            this.errored = errored;
        }

        @Override
        public void run() {
            try {
                if (exiting) {
                    return;
                }
                InputStream is;
                try {
                    is = streamFile.getInputStream();
                    if (streamFile instanceof StreamSourceMeshy.MeshyStreamFile) {
                        is = wrapCompressedStream(is, streamFile.name());
                    }
                } catch (IOException e) {
                    exiting = true;
                    log.warn("Error getting input stream for stream file: " + streamFile, e);
                    return;
                }
                // check again to see if we are exiting now
                if (!exiting) {
                    sourceOpenLock.lock();
                    try {
                        FactoryInputStream.InjectorStreamSource.inject(FactoryInputStream.InjectorStreamSource.DefautlInjectorKey, is);
                        tracker.open(source, errored);
                    } finally {
                        sourceOpenLock.unlock();
                    }
                    TaskDataSource stream = tracker.init(source);
                    if (stream != null) {
                        SourceWrapper wrapper = new SourceWrapper(nextWrapperId.incrementAndGet(), stream, source);
                        wrapperList.add(wrapper);
                        peekerService.submit(new Peeker(wrapper));
                        // may get reset multiple times, only first time matters
                        initialized = true;
                    }
                }
            } finally {
                // need to make sure this gets decremented otherwise we'll never exit
                queuedSourceInitTasks.decrementAndGet();
            }
        }

        private InputStream wrapCompressedStream(InputStream in, String name) throws IOException {
            if (name.endsWith(".gz")) {
                in = IOWrap.gz(in, 4096);
            } else if (name.endsWith(".lzf")) {
                in = new LZFInputStream(in);
            } else if (name.endsWith(".snappy")) {
                in = new SnappyInputStream(in);
            } else if (name.endsWith(".bz2")) {
                in = new BZip2CompressorInputStream(in, true);
            } else if (name.endsWith(".lzma")) {
                in = new LzmaInputStream(in, new Decoder());
            }
            return in;
        }

    }

    /**
     * @exclude
     */
    private class CacheFiller implements Runnable {

        final AtomicBoolean errored;

        public CacheFiller(AtomicBoolean errored) {
            this.errored = errored;
        }

        @Override
        public void run() {
            try {
                while (!exiting && !finished) {
                    fillInputStreamCache(errored);
                    Thread.sleep(cacheFillInterval);
                }
            } catch (InterruptedException e) {
                log.warn("CacheFiller interrupted, likely in shutdown mode");
            }
        }
    }

    /**
     * a simple class to associate a wrapped source with its stateful version.  we'll need to
     * be able to associate the two when switching between sources.
     * <p/>
     * Also provides a source specific lock to prevent multiple threads from calling peek
     * on the source concurrently.  We can't know that the source implementation is thread
     * safe so we need to protect it here.
     *
     * @exclude
     */
    private class SourceWrapper {

        private int id;
        private TaskDataSource source;
        private TaskDataSource ostream;
        private boolean closed;
        final Lock peekLock = new ReentrantLock();

        private SourceWrapper(int id, TaskDataSource source, TaskDataSource ostream) {
            this.id = id;
            this.source = source;
            this.ostream = ostream;
        }

        public TaskDataSource getSource() {
            return source;
        }

        public TaskDataSource getOstream() {
            return ostream;
        }

        private synchronized void close() {
            if (!closed) {
                closed = true;
                source.close();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SourceWrapper that = (SourceWrapper) o;

            if (id != that.id) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }

    void shutdownAndAwaitTermination(ExecutorService... pools) {
        for (ExecutorService pool : pools) {
            pool.shutdownNow(); // Disable new tasks from being submitted
            try {
                // Wait a while for existing tasks to terminate
                if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                    pool.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                        System.err.println("Pool did not terminate");
                    }
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                pool.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }
}
