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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Min;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.addthis.basis.util.LessFiles;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.LessStrings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueString;
import com.addthis.codec.annotations.Time;
import com.addthis.hydra.data.filter.value.StringFilter;
import com.addthis.hydra.store.common.PageFactory;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.PageDB;
import com.addthis.hydra.store.skiplist.ConcurrentPage;
import com.addthis.hydra.task.run.TaskRunConfig;
import com.addthis.hydra.task.source.bundleizer.Bundleizer;
import com.addthis.hydra.task.source.bundleizer.BundleizerFactory;
import com.addthis.hydra.task.stream.PersistentStreamFileSource;
import com.addthis.hydra.task.stream.StreamFile;
import com.addthis.hydra.task.stream.StreamFileSource;
import com.addthis.hydra.task.stream.StreamSourceFiltered;
import com.addthis.hydra.task.stream.StreamSourceHashed;
import com.addthis.hydra.store.compress.CompressedStream;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;

import org.apache.commons.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;

/**
 * Abstract implementation of TaskDataSource
 * <p/>
 * There are many common features that streaming data source needs to deal with
 * and this class performs those tasks so that subclasses can leverage the common
 * functionality.
 * <p/>
 * Stream sources are normally consume data from remote services
 * that run on servers with unbalanced loads and operations against those remote
 * services are subject to network latency and bandwidth constraints.  The goal
 * of this class is to work with all available sources concurrently, pre-opening
 * packets from those sources and queueing them as the packets become available.
 * This allows clients of this class to consume a steady stream of packets from
 * any available host and prevents blocking waits while waiting for slower hosts
 * to make data available.
 * <p/>
 * This class consumes bytes that need to be turned into objects consumers understand.
 * Configuration inputs instruct the class on what type of objects to turn the bytes
 * into on receipt. The {@link BundleizerFactory} is responsible for performing this
 * conversion.
 * <p/>
 * This class maintains persistent state tracking which source files have been
 * consumed.  This enables the class to intelligently ignore upstream files that
 * have already been completely consumed.  The data is maintained in a KV data
 * store using the source file name as the key.
 */
public abstract class AbstractStreamFileDataSource extends TaskDataSource implements BundleFactory {

    private static final Logger log = LoggerFactory.getLogger(AbstractStreamFileDataSource.class);

    // note that some parameters have 'mesh' in the name.  Leaving intact for backwards compatibility
    private static final int MARK_PAGES = Parameter.intValue("source.meshy.mark.pages", 1000);
    private static final int MARK_PAGE_SIZE = Parameter.intValue("source.meshy.mark.pageSize", 20);
    private static final boolean IGNORE_MARKS_ERRORS = Parameter.boolValue("hydra.markdb.error.ignore", false);

    /**
     * A StringFilter that processes file names as a string bundle field. If the filter returns
     * null (false), then the file is skipped. Otherwise, it uses whatever string the filter
     * returns. Anything that isn't a StringFilter throws a runtime error.
     */
    @JsonProperty private StringFilter filter;

    @Time(TimeUnit.MILLISECONDS)
    @JsonProperty private int pollInterval;

    @JsonProperty private int pollCountdown;

    @Time(TimeUnit.SECONDS)
    @JsonProperty private int latchTimeout;

    /** Specifies conversion to bundles. The default is type "channel". */
    @JsonProperty private BundleizerFactory format;

    /** Path to the mark directory. */
    @JsonProperty(required = true) private String markDir;

    /** Ignore the mark directory */
    @JsonProperty private boolean ignoreMarkDir;

    /** Enable metrics visible only from jmx */
    @JsonProperty private boolean jmxMetrics;

    /** Number of shards in the input source. */
    @JsonProperty protected Integer shardTotal;

    /** If specified then process only the shards specified in this array. */
    @JsonProperty protected Integer[] shards;

    /** If true then generate a hash of the filename input rather than use the {{mod}} field. Default is false. */
    @JsonProperty protected boolean hash;

    /**
     * If true then allow all of the Hydra nodes to process all the data when
     * the hash field is false and the filename does not have {{mode}}. Default is false.
     */
    @JsonProperty protected boolean processAllData;

    /** If non-null, then inject the filename into the bundle field using this field name. Default is null. */
    @JsonProperty private AutoField injectSourceName;

    /**
     * Number of bundles to attempt to pull from a file before returning it to the
     * circular file queue. Difficult to understand without looking at the source code,
     * but if you feel the need for a lot of worker threads or are desperate for
     * potential performance gains you might try increasing this. 25 is a good place
     * to start, I think. The default is 1.
     */
    @JsonProperty private int multiBundleReads;

    /**
     * Number of bundles to fetch prior to starting the worker threads.
     * Default is either "dataSourceMeshy2.preopen" configuration value or 2.
     */
    @JsonProperty private int preOpen;

    /**
     * Trigger an error when the number of skipped sources is greater than this value.
     * Default is either "dataSourceMeshy2.skipSourceExit" configuration value or 0.
     */
    @JsonProperty private int skipSourceExit;

    /**
     * Maximum size of the queue that stores bundles prior to their processing.
     * Default is either "dataSourceMeshy2.buffer" configuration value or 128.
     */
    @JsonProperty(required = true) private int buffer;

    /**
     * Number of worker threads that request data from the meshy source.
     * Default is either "dataSourceMeshy2.workers" configuration value or 2.
     */
    @Min(1)
    @JsonProperty(required = true) private int workers;

    /**
     * Set to enable marks compatibility mode with older source types. eg. 'mesh' for mesh1 and
     * 'stream' for stream2. 'stream2' is also fine. Do not set to anything unless doing an in-place
     * conversion of an existing job with an older source type. It won't be damaging for new jobs, but
     * it would be nice to eventually be able to drop support entirely. If cloning a job with this set,
     * then please remove it from the clone (before running).
     * <p/>
     * In more detail: Any non-null value will use legacy marks and anything beginning with
     * 'stream' will trim the starting '/' in a mesh path.
     */
    @JsonProperty private String legacyMode;

    @JsonProperty(required = true) private int magicMarksNumber;

    @JsonProperty private TaskRunConfig config;

    private final ListBundleFormat bundleFormat = new ListBundleFormat();
    private final ExecutorService workerThreadPool = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE, 5L, TimeUnit.SECONDS, new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("streamSourceWorker-%d").build());

    /* metrics */
    private final Histogram queueSizeHisto = Metrics.newHistogram(getClass(), "queueSizeHisto");
    private final Histogram fileSizeHisto = Metrics.newHistogram(getClass(), "fileSizeHisto");
    private final Timer readTimer = Metrics.newTimer(getClass(), "readTimer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Counter openNew = Metrics.newCounter(getClass(), "openNew");
    private final Counter openIndex = Metrics.newCounter(getClass(), "openIndex");
    private final Counter openSkip = Metrics.newCounter(getClass(), "openSkip");
    private final Counter skipping = Metrics.newCounter(getClass(), "skipping");
    private final Counter reading = Metrics.newCounter(getClass(), "reading");
    private final Counter opening = Metrics.newCounter(getClass(), "opening");

    // Concurrency provisions
    private final Counter globalBundleSkip = Metrics.newCounter(getClass(), "globalBundleSkip");
    private final AtomicInteger consecutiveFileSkip = new AtomicInteger();
    private final ThreadLocal<Integer> localBundleSkip =
            new ThreadLocal<Integer>() {
                @Override
                protected Integer initialValue() {
                    return 0;
                }
            };

    // State control
    private final LinkedBlockingQueue<Wrap> preOpened = new LinkedBlockingQueue<>();
    protected final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    protected final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final CountDownLatch initialized = new CountDownLatch(1);
    private boolean localInitialized = false;

    private BlockingQueue<Bundle> queue;
    private PageDB<SimpleMark> markDB;
    private File markDirFile;
    private CompletableFuture<Void> aggregateWorkerFuture;
    private boolean useSimpleMarks = false;
    private boolean useLegacyStreamPath = false;

    private StreamFileSource source;

    public AbstractStreamFileDataSource() {}

    public File getMarkDirFile() {
        return markDirFile;
    }

    public void setSource(StreamFileSource source) {
        AbstractStreamFileDataSource.this.source = source;
    }

    protected abstract PersistentStreamFileSource getSource();

    @Override public Bundle createBundle() {
        return new ListBundle(bundleFormat);
    }

    @Override public void init() {
        if (legacyMode != null) {
            magicMarksNumber = 0;
            useSimpleMarks = true;
            if (legacyMode.startsWith("stream")) {
                log.info("Using legacy mode for 'stream2' marks");
                useLegacyStreamPath = true;
            } else {
                log.info("Using legacy mode for 'mesh' marks");
            }
        }
        try {
            if (ignoreMarkDir) {
                File md = new File(markDir);

                if (md.exists()) {
                    FileUtils.deleteDirectory(md);
                    log.info("Deleted marks directory : {}", md);
                }
            }

            markDirFile = LessFiles.initDirectory(markDir);
            if (useSimpleMarks) {
                PageFactory<DBKey, SimpleMark> factory = ConcurrentPage.ConcurrentPageFactory.singleton;
                markDB = new PageDB<>(markDirFile, SimpleMark.class,
                        MARK_PAGE_SIZE, MARK_PAGES, factory);
            } else {
                PageFactory<DBKey, SimpleMark> factory = ConcurrentPage.ConcurrentPageFactory.singleton;
                markDB = new PageDB<>(markDirFile,
                        Mark.class, MARK_PAGE_SIZE, MARK_PAGES, factory);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (shardTotal == null || shardTotal == 0) {
            shardTotal = config.nodeCount;
        }
        if (shards == null) {
            shards = config.calcShardList(shardTotal);
        }
        PersistentStreamFileSource persistentStreamFileSource = getSource();
        source = persistentStreamFileSource;
        if (!processAllData &&
            !hash &&
            !((persistentStreamFileSource != null) && persistentStreamFileSource.hasMod())) {
            log.error("possible source misconfiguration.  lacks both 'hash' and '{{mod}}'.  fix or set processAllData:true");
            throw new RuntimeException("Possible Source Misconfiguration");
        }
        try {
            if (persistentStreamFileSource != null) {
                if (!persistentStreamFileSource.init(getMarkDirFile(), shards)) {
                    throw new IllegalStateException("Failure to initialize input source");
                }
            }
            if (filter != null) {
                setSource(new StreamSourceFiltered(source, filter));
            }
            if (hash) {
                setSource(new StreamSourceHashed(source, shards, shardTotal, useLegacyStreamPath));
            }
            log.info("buffering[capacity={};workers={};preopen={};marks={};maxSkip={};shards={}]",
                     buffer, workers, preOpen, markDir, skipSourceExit, LessStrings.join(shards, ","));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        queue = new LinkedBlockingQueue<>(buffer);

        List<CompletableFuture<Void>> workerFutures = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            Runnable sourceWorker = new SourceWorker(i);
            workerFutures.add(runAsync(sourceWorker, workerThreadPool).whenComplete((ignored, error) -> {
            if (error != null) {
                shuttingDown.set(true);
                closeFuture.completeExceptionally(error);
            }}));
        }
        aggregateWorkerFuture = allOf(workerFutures.toArray(new CompletableFuture[workerFutures.size()]));
        aggregateWorkerFuture.thenRunAsync(this::close);
    }

    @Nullable @Override public Bundle next() throws DataChannelError {
        if ((skipSourceExit > 0) && (consecutiveFileSkip.get() >= skipSourceExit)) {
            throw new DataChannelError("skipped too many sources: " + skipSourceExit + ".  please check your job config.");
        }
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
        return populateToString(MoreObjects.toStringHelper(this));
    }

    private String fileStatsToString(String reason) {
        return populateToString(MoreObjects.toStringHelper(reason));
    }

    private String populateToString(MoreObjects.ToStringHelper helper) {
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

    @Override
    public void close() {
        if (shuttingDown.compareAndSet(false, true)) {
            log.info("closing stream file data source. preOpened={} queue={}", preOpened.size(), queue.size());
            try {
                log.info("Waiting up to {} seconds for outstanding worker tasks to complete.", latchTimeout);
                getUninterruptibly(aggregateWorkerFuture, latchTimeout, TimeUnit.SECONDS);
                log.info("All threads have finished.");

                log.debug("closing wrappers");
                closePreOpenedQueue();

                log.debug("shutting down mesh");
                //we may overwrite the local source variable and in doing so throw away the Persistance flag
                PersistentStreamFileSource baseSource = getSource();
                if (baseSource != null) {
                    baseSource.shutdown();
                } else {
                    log.warn("getSource() returned null and no source was shutdown");
                }

                closeMarkDB();
                log.info(fileStatsToString("shutdown complete"));
            } catch (IOException ex) {
                UncheckedIOException unchecked = new UncheckedIOException(ex);
                closeFuture.completeExceptionally(unchecked);
                throw unchecked;
            } catch (Throwable t) {
                closeFuture.completeExceptionally(t);
                throw propagate(t);
            }
            workerThreadPool.shutdown();
            closeFuture.complete(null);
        } else {
            try {
                closeFuture.join();
            } catch (CompletionException ex) {
                throw propagate(ex.getCause());
            }
        }
    }

    protected void closePreOpenedQueue() throws IOException {
        for (Wrap wrap : preOpened) {
            wrap.close(false);
        }
    }

    protected void closeMarkDB() {
        if (markDB != null) {
            markDB.close();
        } else {
            log.warn("markdb was null, and was not closed");
        }
    }

    private class SourceWorker implements Runnable {
        private final int workerId;

        public SourceWorker(int workerId) {
            this.workerId = workerId;
        }

        @Override
        public void run() {
            log.debug("worker {} starting", workerId);
            try {
                // preopen a number of sources
                int preOpenSize = Math.max(1, preOpen / workers);
                for (int i = 0; i < preOpenSize; i++) {
                    Wrap preOpenedWrap = nextWrappedSource();
                    log.debug("pre-init {}", preOpenedWrap);
                    if (preOpenedWrap == null) {
                        break;
                    }
                    preOpened.put(preOpenedWrap);
                }

                //fill already has a while loop that checks shuttingDown
                fill();
            } catch (Exception e) {
                log.warn("Exception while running data source meshy worker thread.", e);
                if (!IGNORE_MARKS_ERRORS) {
                    throw propagate(e);
                }
            } finally {
                log.debug("worker {} exiting shuttingDown={}", workerId, shuttingDown);
            }
        }

        @Nullable private Wrap nextWrappedSource() throws IOException {
            // this short circuit is not functionally required, but is a bit neater
            if (shuttingDown.get()) {
                return null;
            }
            StreamFile stream = source.nextSource();
            if (stream == null) {
                return null;
            }
            return new Wrap(stream);
        }

        private void fill() throws Exception {
            @Nullable Wrap wrap = null;
            try {
                while (!shuttingDown.get()) {
                    wrap = preOpened.poll(); //take if immediately available
                    if (wrap == null) {
                        return; //exits worker thread
                    }
                    if (!multiFill(wrap, multiBundleReads)) {
                        wrap = nextWrappedSource();
                    }
                    if (wrap != null) { //May be null from nextWrappedSource -> decreases size of preOpened
                        preOpened.put(wrap);
                    }
                    wrap = null;
                }
                log.debug("[{}] read", workerId);
            } finally {
                if (wrap != null) {
                    wrap.close(false);
                }
            }
        }

        private boolean multiFill(Wrap wrap, int fillCount) throws IOException, InterruptedException {
            for (int i = 0; i < fillCount; i++) {
                Bundle next = wrap.next();
                // is source exhausted?
                if (next == null) {
                    return false;
                }
                while (!queue.offer(next, 1, TimeUnit.SECONDS)) {
                    if (shuttingDown.get()) {
                        wrap.close(false);
                        return false;
                    }
                }
                wrap.mark.setIndex(wrap.mark.getIndex() + 1);
                if (jmxMetrics) {
                    queueSizeHisto.update(queue.size());
                }
                // may get called multiple times but only first call matters
                if (!localInitialized) {
                    initialized.countDown();
                    localInitialized = true;
                }
            }
            return true;
        }
    }

    protected class Wrap {

        final DBKey dbKey;
        final StreamFile stream;
        final ValueString sourceName;
        InputStream input;
        Bundleizer bundleizer;
        boolean closed;
        SimpleMark mark;

        Wrap(StreamFile stream) throws IOException {
            fileSizeHisto.update(stream.length());
            this.stream = stream;
            String keyString = stream.getPath();
            if (useLegacyStreamPath && keyString.charAt(0) == '/') {
                keyString = keyString.substring(1);
            }
            this.dbKey = new DBKey(magicMarksNumber, keyString);
            this.sourceName = ValueFactory.create(stream.getPath());
            mark = markDB.get(dbKey);
            String stateValue = Mark.calcValue(stream);
            if (mark == null) {
                if (useSimpleMarks) {
                    mark = new SimpleMark().set(stateValue, 0);
                } else {
                    mark = new Mark().set(stateValue, 0);
                }
                log.debug("mark.init {} / {}", mark, stream);
                openNew.inc();
                opening.inc();
                input = stream.getInputStream();
            } else {
                if (mark.getValue().equals(stateValue) && mark.isEnd()) {
                    log.debug("mark.skip {} / {}", mark, stream);
                    openSkip.inc();
                    if (skipSourceExit > 0) {
                        consecutiveFileSkip.incrementAndGet();
                    }
                    closed = true;
                    return;
                } else {
                    openIndex.inc();
                    if (skipSourceExit > 0) {
                        consecutiveFileSkip.set(0);
                    }
                }
                opening.inc();
                input = stream.getInputStream();
            }
            reading.inc();
        }

        void maybeFinishInit() throws IOException {
            if (bundleizer == null) {
                input = CompressedStream.decompressInputStream(input, stream.name()); // blocks waiting for network (if compressed)
                opening.dec();
                bundleizer = format.createBundleizer(input, AbstractStreamFileDataSource.this);
                long read = mark.getIndex();
                if (read == 0) {
                    return;
                }
                int bundlesSkipped = 0;
                skipping.inc();
                while (read > 0) {
                    if (++bundlesSkipped % 100 == 0) {
                        int totalSkip = localBundleSkip.get() + bundlesSkipped;
                        if ((totalSkip / 100) % 250 == 0) {
                            log.info(MoreObjects.toStringHelper(Thread.currentThread().getName() + " bundle skip log")
                                            .add("thread-skip", totalSkip)
                                            .add("file-skip", bundlesSkipped)
                                            .add("file-to-skip", read)
                                            .add("global-skip-estimate", bundlesSkipped + globalBundleSkip.count())
                                            .toString());
                            localBundleSkip.set(totalSkip);
                            globalBundleSkip.inc(bundlesSkipped);
                            bundlesSkipped = 0;
                        }
                    }
                    read--;

                    if (shuttingDown.get() || (bundleizer.next() == null)) {
                        close(false);
                        break;
                    }
                }
                skipping.dec();
                localBundleSkip.set(localBundleSkip.get() + bundlesSkipped);
                globalBundleSkip.inc(bundlesSkipped);
                log.debug("mark.indx {} / {}", mark, stream);
            }
        }

        void close(boolean wasEnd) throws IOException {
            if (!closed) {
                mark.setEnd(wasEnd);
                input.close();
                mark.update(stream);
                markDB.put(dbKey, mark);
                log.debug("mark.save {}:{} / {}", dbKey, mark, stream);
                closed = true;
                reading.dec();
            }
        }

        @Nullable Bundle next() throws IOException {
            if (closed) {
                log.debug("next {} / {} CLOSED returns null", mark, stream);
                return null;
            }
            try {
                maybeFinishInit();
                Bundle next = bundleizer.next();
                log.debug("next {} / {} = {}", mark, stream, next);
                if (next == null) {
                    close(true);
                } else {
                    if (injectSourceName != null) {
                        injectSourceName.setValue(next, sourceName);
                    }
                }
                return next;
            } catch (Exception ex) {
                if (!IGNORE_MARKS_ERRORS) {
                    log.error("(rethrowing) source error with mark: {}, stream file: {}", mark, stream, ex);
                    throw ex;
                }
                log.warn("source error with mark: {}, stream file: {}", mark, stream, ex);
                mark.setError(mark.getError() + 1);
                close(false);
            }
            return null;
        }
    }

    @Nonnull @Override
    public ImmutableList<Path> writableRootPaths() {
        return ImmutableList.of(Paths.get(markDir));
    }

}
