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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.Files;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueString;
import com.addthis.codec.Codec;
import com.addthis.hydra.common.plugins.PluginReader;
import com.addthis.hydra.data.filter.value.StringFilter;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.PageDB;
import com.addthis.hydra.task.run.TaskRunConfig;
import com.addthis.hydra.task.source.bundleizer.ChannelBundleizer;
import com.addthis.hydra.task.stream.PersistentStreamFileSource;
import com.addthis.hydra.task.stream.StreamFile;
import com.addthis.hydra.task.stream.StreamFileSource;
import com.addthis.hydra.task.stream.StreamSourceFiltered;
import com.addthis.hydra.task.stream.StreamSourceHashed;

import com.google.common.base.Objects;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;

import org.apache.commons.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class AbstractStreamFileDataSource extends TaskDataSource implements BundleFactory, Runnable {

    private static final Logger log = LoggerFactory.getLogger(AbstractStreamFileDataSource.class);

    // note that some parameters have 'mesh' in the name.  Leaving intact for backwards compatibility
    private static final int POLL_INTERVAL = Parameter.intValue("dataSourceMeshy2.pollInterval", 1000);
    private static final int POLL_COUNTDOWN = Parameter.intValue("dataSourceMeshy2.pollCountdown", 1800); // 30 minutes
    private static final int DEFAULT_PREOPEN = Parameter.intValue("dataSourceMeshy2.preopen", 1);
    private static final int DEFAULT_SKIP_SOURCE_EXIT = Parameter.intValue("dataSourceMeshy2.skipSourceExit", 0);
    private static final int MARK_PAGES = Parameter.intValue("source.meshy.mark.pages", 1000);
    private static final int MARK_PAGE_SIZE = Parameter.intValue("source.meshy.mark.pageSize", 20);
    private static final int DEFAULT_WORKERS = Parameter.intValue("dataSourceMeshy2.workers", 2);
    private static final int DEFAULT_BUFFER = Parameter.intValue("dataSourceMeshy2.buffer", 128);
    private static final int DEFAULT_COUNTDOWN_LATCH_TIMEOUT = Parameter.intValue("dataSourceMeshy2.timeout.sec", 30);

    /**
     * A StringFilter that processes file names as a string bundle field. If the filter returns
     * null (false), then the file is skipped. Otherwise, it uses whatever string the filter
     * returns. Anything that isn't a StringFilter throws a runtime error.
     */
    @Codec.Set(codable = true)
    private StringFilter filter;

    /**
     * Specifies conversion to bundles.
     */
    @Codec.Set(codable = true)
    private BundleizerFactory format = new ChannelBundleizer();

    /**
     * Path to the mark directory.
     */
    @Codec.Set(codable = true)
    private String markDir = "marks";

    /**
     * Ignore the mark directory
     */
    @Codec.Set(codable = true)
    private boolean ignoreMarkDir;

    /**
     * Enable metrics visible only from jmx
     */
    @Codec.Set(codable = true)
    private boolean jmxMetrics;

    /**
     * Number of shards in the input source.
     */
    @Codec.Set(codable = true)
    protected Integer shardTotal;

    /**
     * If specified then process only the shards specified in this array.
     */
    @Codec.Set(codable = true)
    protected Integer shards[];

    /**
     * If true then generate a hash of the filename input rather than use the {{mod}} field. Default is false.
     */
    @Codec.Set(codable = true)
    protected boolean hash;

    /**
     * If true then allow all of the Hydra nodes to process all the data when
     * the hash field is false and the filename does not have {{mode}}. Default is false.
     */
    @Codec.Set(codable = true)
    protected boolean processAllData;

    /**
     * If non-null, then inject the filename into the bundle field using this field name. Default is null.
     */
    @Codec.Set(codable = true)
    private String injectSourceName;

    /**
     * Number of bundles to attempt to pull from a file before returning it to the
     * circular file queue. Difficult to understand without looking at the source code,
     * but if you feel the need for a lot of worker threads or are desperate for
     * potential performance gains you might try increasing this. 25 is a good place
     * to start, I think. The default is 1.
     */
    @Codec.Set(codable = true)
    private int multiBundleReads = 1;

    /**
     * Number of bundles to fetch prior to starting the worker threads.
     * Default is either "dataSourceMeshy2.preopen" configuration value or 2.
     */
    @Codec.Set(codable = true)
    private int preOpen = DEFAULT_PREOPEN;

    /**
     * Trigger an error when the number of skipped sources is greater than this value.
     * Default is either "dataSourceMeshy2.skipSourceExit" configuration value  or 0.
     */
    @Codec.Set(codable = true)
    private int skipSourceExit = DEFAULT_SKIP_SOURCE_EXIT;

    /**
     * Maximum size of the queue that stores bundles prior to their processing.
     * Default is either "dataSourceMeshy2.buffer" configuration value or 128.
     */
    @Codec.Set(codable = true)
    private int buffer = DEFAULT_BUFFER;

    /**
     * Number of worker threads that request data from the meshy source.
     * Default is either "dataSourceMeshy2.workers" configuration value or 2.
     */
    @Codec.Set(codable = true)
    private int workers = DEFAULT_WORKERS;

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
    @Codec.Set(codable = true)
    private String legacyMode;

    private final ListBundleFormat bundleFormat = new ListBundleFormat();
    private final Bundle termBundle = new ListBundle(bundleFormat);

    /* metrics */
    private Histogram queueSizeHisto = Metrics.newHistogram(getClass(), "queueSizeHisto");
    private Histogram fileSizeHisto = Metrics.newHistogram(getClass(), "fileSizeHisto");
    private Timer readTimer = Metrics.newTimer(getClass(), "readTimer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Counter openNew = Metrics.newCounter(getClass(), "openNew");
    private final Counter openIndex = Metrics.newCounter(getClass(), "openIndex");
    private final Counter openSkip = Metrics.newCounter(getClass(), "openSkip");
    private final Counter skipping = Metrics.newCounter(getClass(), "skipping");
    private final Counter reading = Metrics.newCounter(getClass(), "reading");
    private final Counter opening = Metrics.newCounter(getClass(), "opening");

    // Concurrency provisions
    private final AtomicInteger generateThreadIDs = new AtomicInteger(0);
    private CountDownLatch runningThreadCountDownLatch = null;
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
    protected final AtomicBoolean done = new AtomicBoolean(false);
    protected final AtomicBoolean exiting = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private CountDownLatch initialized = new CountDownLatch(1);
    private boolean localInitialized = false;

    private BlockingQueue<Bundle> queue;
    private PageDB<SimpleMark> markDB;
    private BundleField injectSourceField;
    private File markDirFile;
    private int magicMarksNumber = 42;
    private boolean useSimpleMarks = false;
    private boolean useLegacyStreamPath = false;

    private StreamFileSource source;
    private final Object nextSourceLock = new Object();

    public AbstractStreamFileDataSource() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                exiting.set(true);
                shutdown();
            }
        });
    }

    public File getMarkDirFile() {
        return markDirFile;
    }

    public void setSource(StreamFileSource source) {
        this.source = source;
    }

    protected abstract PersistentStreamFileSource getSource();

    protected void pushTermBundle() {
        if (queue != null) {
            boolean setDone = done.compareAndSet(false, true);
            boolean pushedTerm = queue.offer(termBundle);
            log.info("initiating shutdown(). setDone={} done={} preOpened={} exiting={} pushedTerm={} queue={} moreData={}",
                    setDone, done.get(), preOpened.size(), exiting.get(), pushedTerm, queue.size(), hadMoreData());
        }
    }

    protected void closePreOpenedQueue() {
        for (Wrap wrap : preOpened) {
            try {
                wrap.close();
            } catch (IOException e) {
                log.warn("", e);
            }
        }
    }

    protected void closeMarkDB() {
        if (markDB != null) {
            markDB.close();
        } else {
            log.warn("markdb was null, and was not closed");
        }
    }

    @Override
    protected void open(TaskRunConfig config) {
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
                    log.warn("Deleted file : {}", md);
                }
            }

            markDirFile = Files.initDirectory(markDir);
            if (useSimpleMarks) {
                markDB = new PageDB<>(markDirFile, SimpleMark.class, MARK_PAGE_SIZE, MARK_PAGES);
            } else {
                markDB = new PageDB<SimpleMark>(markDirFile, Mark.class, MARK_PAGE_SIZE, MARK_PAGES);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (injectSourceName != null) {
            injectSourceField = bundleFormat.getField(injectSourceName);
        }
        if (shardTotal == null || shardTotal == 0) {
            shardTotal = config.nodeCount;
        }
        if (shards == null) {
            shards = config.calcShardList(shardTotal);
        }
        source = getSource();
        PersistentStreamFileSource persistentStreamFileSource = null;
        if (source != null) {
            persistentStreamFileSource = (PersistentStreamFileSource) source;
        }
        if (!processAllData && !hash && !(persistentStreamFileSource != null && persistentStreamFileSource.hasMod())) {
            log.error("possible source misconfiguration.  lacks both 'hash' and '{{mod}}'.  fix or set processAllData:true");
            throw new RuntimeException("Possible Source Misconfiguration");
        }
        try {
            if (persistentStreamFileSource != null) {
                persistentStreamFileSource.init(getMarkDirFile(), shards);
            }
            if (filter != null) {
                setSource(new StreamSourceFiltered(source, filter));
            }
            if (hash) {
                setSource(new StreamSourceHashed(source, shards, shardTotal, useLegacyStreamPath));
            }
            log.info("buffering[capacity={};workers={};preopen={};marks={};maxSkip={};shards={}]", buffer, workers,
                    preOpen, markDir, skipSourceExit, Strings.join(shards, ","));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        queue = new ArrayBlockingQueue<>(buffer);

        if (workers == 0) {
            log.error("Either we failed to find any meshy sources or workers was set to 0. Shutting down.");
            shutdown();
        }
        runningThreadCountDownLatch = new CountDownLatch(workers);
        int workerId = workers;
        while (workerId-- > 0) {
            new Thread(this, "DataSourceMeshyWorker-" + workerId).start();
        }
    }

    protected void shutdownBody() {
        // shutdown adds termBundle to queue
        pushTermBundle();
        if (runningThreadCountDownLatch != null) {
            boolean success = false;
            log.info("Waiting up to {} seconds for outstanding threads to complete.",
                    DEFAULT_COUNTDOWN_LATCH_TIMEOUT);
            try {
                success = runningThreadCountDownLatch.await(DEFAULT_COUNTDOWN_LATCH_TIMEOUT,
                        TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.warn("", e);
            }
            if (success) {
                log.info("All threads have finished.");
            } else {
                log.info("All threads did NOT finish.");
            }
        }
        log.debug("closing wrappers");
        closePreOpenedQueue();
        log.debug("shutting down mesh");
        //we may overwrite the local source variable and in doing so throw away the Persistance flag
        PersistentStreamFileSource baseSource = getSource();
        if (baseSource != null) {
            try {
                baseSource.shutdown();
            } catch (IOException e) {
                log.warn("", e);
            }
        } else {
            log.warn("getSource() returned null and no source was shutdown");
        }

        closeMarkDB();
        log.info(fileStatsToString("shutdown complete"));
    }

    protected void shutdown() {
        if (!shutdown.getAndSet(true)) {
            /**
             * The body of the shutdown method has been moved into its own
             * method for testing purposes. Tests which must wait until the
             * shutdown has completed will override the shutdownBody() method
             * and place synchronization constructs at the end of the overridden
             * method.
             */
            shutdownBody();
        }
    }

    @Override
    public void run() {
        int threadID = generateThreadIDs.incrementAndGet();
        log.debug("worker {} starting", threadID);
        try {
            // preopen a number of sources
            int preOpenSize = Math.max(1, preOpen / workers);
            for (int i = 0; i < preOpenSize; i++) {
                Wrap preOpenedWrap = nextWrappedSource();
                log.debug("pre-open {}", preOpenedWrap);
                if (preOpenedWrap == null) {
                    break;
                }
                preOpened.put(preOpenedWrap);
            }

            //fill already has a while loop that checks done
            fill(threadID);
        } catch (Exception e) {
            log.warn("Exception while running data source meshy worker thread.", e);
        } finally {
            log.debug("worker {} exiting done={}", threadID, done);
            runningThreadCountDownLatch.countDown();

            /**
             * This expression can evaluate to true more than once.
             * It is OK because the shutdown() method has a guard
             * to ensure it is invoked at most once.
             */
            if (runningThreadCountDownLatch.getCount() == 0) {
                log.info("No more workers are running. One or more threads will attempt to call shutdown.");
                shutdown();
            }
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
                log.debug("mark.open {} / {}", mark, stream);
                opening.inc();
                input = stream.getInputStream(); //blocks waiting for network
                opening.dec();
                bundleizer = format.createBundleizer(input, AbstractStreamFileDataSource.this);
                openNew.inc();
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
                input = stream.getInputStream(); //blocks waiting for network
                opening.dec();
                bundleizer = format.createBundleizer(input, AbstractStreamFileDataSource.this);
                long read = mark.getIndex();
                int bundlesSkipped = 0;
                skipping.inc();
                while (read > 0) {
                    if (++bundlesSkipped % 100 == 0) {
                        int totalSkip = localBundleSkip.get() + bundlesSkipped;
                        if ((totalSkip / 100) % 250 == 0) {
                            log.info(Objects.toStringHelper(Thread.currentThread().getName() + " bundle skip log")
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
                    try {
                        bundleizer.next();
                        read--;
                    } catch (Exception e) {
                        log.warn("", e);
                    }
                }
                skipping.dec();
                localBundleSkip.set(localBundleSkip.get() + bundlesSkipped);
                globalBundleSkip.inc(bundlesSkipped);
                log.debug("mark.indx {} / {}", mark, stream);
            }
            reading.inc();
        }

        void close() throws IOException {
            if (!closed) {
                input.close();
                mark.update(stream);
                markDB.put(dbKey, mark);
                log.debug("mark.save {}:{} / {}", dbKey, mark, stream);
                closed = true;
                reading.dec();
            }
        }

        Bundle next() throws IOException {
            if (closed) {
                log.debug("next {} / {} CLOSED returns null", mark, stream);
                return null;
            }
            Bundle next = null;
            try {
                next = bundleizer.next();
                log.debug("next {} / {} = {}", mark, stream, next);
                if (next == null) {
                    mark.setEnd(true);
                    close();
                } else {
                    mark.setIndex(mark.getIndex() + 1);
                    if (injectSourceField != null) {
                        next.setValue(injectSourceField, sourceName);
                    }
                }
            } catch (Exception ex) {
                log.info("error {} / {}", mark, stream, ex);
                mark.setError(mark.getError() + 1);
                close();
            }
            return next;
        }
    }

    private Wrap nextWrappedSource() throws IOException {
        StreamFile stream;
        synchronized (nextSourceLock) {
            stream = source.nextSource();
        }
        if (stream == null) {
            return null;
        }
        return new Wrap(stream);

    }

    private boolean multiFill(Wrap wrap, int fillCount) throws Exception {
        for (int i = 0; i < fillCount; i++) {
            Bundle next = wrap.next();
            if (next == null) //is source exhausted?
            {
                return false;
            }
            // looks like we can drop a bundle on done == true
            while (!queue.offer(next, 1, TimeUnit.SECONDS) && !done.get()) {
            }
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

    private void fill(int threadID) {
        Wrap wrap = null;
        try {
            while (!done.get() && !exiting.get()) {
                wrap = preOpened.poll(); //take if immediately available
                if (wrap == null) {
                    return; //exits worker thread
                }
                if (!multiFill(wrap, multiBundleReads)) {
                    wrap = nextWrappedSource();
                }
                if (wrap != null) //May be null from nextWrappedSource -> decreases size of preOpened
                {
                    preOpened.put(wrap);
                }
                wrap = null;
            }
            log.debug("[{}] read", threadID);
        } catch (Exception ex) {
            log.warn("", ex);
        } finally {
            if (wrap != null) {
                try {
                    wrap.close();
                } catch (Exception ex) {
                    log.warn("", ex);
                }
            }
        }
    }

    @Override
    public Bundle next() throws DataChannelError {
        if (skipSourceExit > 0 && consecutiveFileSkip.get() >= skipSourceExit) {
            throw new DataChannelError("skipped too many sources: " + skipSourceExit + ".  please check your job config.");
        }
        try {
            int countdown = POLL_COUNTDOWN;
            while ((localInitialized || waitForInitialized()) && POLL_COUNTDOWN == 0 || countdown-- > 0) {
                long startTime = jmxMetrics ? System.currentTimeMillis() : 0;
                Bundle next = queue.poll(POLL_INTERVAL, TimeUnit.MILLISECONDS);
                if (jmxMetrics) {
                    readTimer.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
                }
                if (next == termBundle) {
                    // re-add TERM bundle in case there are more consumers
                    if (!queue.offer(next)) {
                        log.info("next offer TERM fail. queue={}", queue.size());
                    }
                    return null;
                } else if (next != null) {
                    return next;
                }
                if (done.get()) {
                    return null;
                }
                if (POLL_COUNTDOWN > 0) {
                    log.info("next polled null, retrying {} more times. done={} exiting={}", countdown, done.get(), exiting.get());
                }
                log.info(fileStatsToString("null poll "));
            }
            if (countdown < 0) {
                log.info("exit with no data during poll countdown");
            }
            return null;
        } catch (Exception ex) {
            throw new DataChannelError(ex);
        }
    }

    private String fileStatsToString(String reason) {
        return Objects.toStringHelper(reason)
                .add("reading", reading.count())
                .add("opening", opening.count())
                .add("unseen", openNew.count())
                .add("continued", openIndex.count())
                .add("skipping", skipping.count())
                .add("skipped", openSkip.count())
                .add("bundles-skipped", globalBundleSkip.count())
                .add("median-size", fileSizeHisto.getSnapshot().getMedian())
                .toString();
    }

    @Override
    public Bundle peek() throws DataChannelError {
        if (localInitialized || waitForInitialized()) {
            try {
                Bundle peek = queue.peek();
                return peek == termBundle ? null : peek;
            } catch (Exception ex) {
                throw new DataChannelError(ex);
            }
        }
        return null;
    }

    private boolean waitForInitialized() {
        try {
            while (!localInitialized && !initialized.await(3, TimeUnit.SECONDS) && !done.get()) {
                log.info(fileStatsToString("waiting for initialization"));
            }
            log.info(fileStatsToString("initialized"));
            localInitialized = true;
            return true;
        } catch (InterruptedException ignored) {
            log.info("interrupted while waiting for initialization to be true");
            return false;
        }
    }

    @Override
    public void close() {
        if (log.isDebugEnabled() || done.get()) {
            log.info("close() called. done={} queue={}", done, queue.size());
        }
        done.set(true);
    }

    @Override
    public Bundle createBundle() {
        return new ListBundle(bundleFormat);
    }

    /**
     * chops inputstreams into bundles
     */
    public interface Bundleizer {

        public Bundle next() throws Exception;
    }

    /**
     * Specifies the conversion into bundles (this is specific to mesh2).
     * <p>The following factories are available:
     * <ul>
     * <li>{@link ChannelBundleizer channel}</li>
     * <li>{@link com.addthis.hydra.task.source.bundleizer.ColumnBundleizer column}</li>
     * <li>{@link com.addthis.hydra.task.source.bundleizer.KVBundleizer kv}</li>
     * </ul>
     *
     * @user-reference
     */
    @Codec.Set(classMapFactory = BundleizerFactory.CMAP.class)
    public abstract static class BundleizerFactory implements Codec.Codable {

        private static Codec.ClassMap cmap = new Codec.ClassMap() {
            @Override
            public String getClassField() {
                return "type";
            }
        };

        public static class CMAP implements Codec.ClassMapFactory {

            public Codec.ClassMap getClassMap() {
                return cmap;
            }
        }

        static {
            PluginReader.registerPlugin("-stream-bundleizer.classmap", cmap, BundleizerFactory.class);
        }

        public abstract Bundleizer createBundleizer(InputStream input, BundleFactory factory);
    }

}
