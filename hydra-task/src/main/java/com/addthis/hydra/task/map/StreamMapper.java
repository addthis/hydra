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
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;

import java.net.ServerSocket;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import java.nio.file.Path;
import java.text.SimpleDateFormat;

import com.addthis.basis.jmx.MBeanRemotingSupport;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.codec.annotations.Time;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.data.filter.bundle.BundleFilterDebugPrint;
import com.addthis.hydra.task.output.TaskDataOutput;
import com.addthis.hydra.task.run.TaskExitState;
import com.addthis.hydra.task.run.TaskRunnable;
import com.addthis.hydra.task.source.TaskDataSource;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static java.lang.String.join;

/**
 * <p>This is <span class="hydra-summary">the most common form of Hydra job (either a split job or a map job)</span>.
 * It is specified with {@code type : "map"}.</p>
 * <p>There are two common use cases of these jobs:</p>
 * <ul>
 * <li><p>Split jobs. These jobs take in lines of data, such as log files, and emit new lines.
 * It might change the format (text in, binary out),
 * or drop lines that fail to some predicate, or create multiple derived lines from each input,
 * or make all strings lowercase, or other arbitrary transformations.
 * But it's always lines in, lines out.</li>
 * <li>Tree jobs. These jobs take in log lines of input,
 * such as just emitted by a split job, and build a tree representation of the data.
 * This hierarchical databases can then be explored through the distribute Hydra query system.</li>
 * </ul>
 * <p>Example:</p>
 * <pre>
 * map.source: {...}
 * map.filterIn: {...}
 * map.filterOut: {...}
 * map.output: {...}
 * </pre>
 *
 * @user-reference
 */
public class StreamMapper implements StreamEmitter, TaskRunnable {
    private static final Logger log = LoggerFactory.getLogger(StreamMapper.class);

    /** The data source for this job. */
    private final TaskDataSource source;

    /** The transformations to apply onto the data. */
    private final MapDef map;

    /** The data sink for emitting the result of the transformations. */
    private final TaskDataOutput output;

    /**
     * Allow more flexible stream builders.  For example, asynchronous or builders
     * where one or more bundles roll up into a single bundle or a single bundle causes
     * the emission of multiple bundles.
     */
    private final StreamBuilder builder;

    /** Print to the console statistics while processing the data. Default is {@code true}. */
    private final boolean stats;

    /** How frequently statistics should be printed. */
    private final long metricTick;

    /**
     * If true then ensure that writable directories are all unique.
     * Default is true.
     **/
    private final boolean validateDirs;

    /**
     * Duration to wait for outstanding processor tasks
     * to finish on shutdown. Default is "60 seconds"
     */
    private final int taskFinishTimeout;

    /**
     * Use MapFeederForkJoin if true; Otherwise use original MapFeeder.
     *
     * Default is false. This is a temporary flag that allows us to selectively test the
     * performance of the new fork join map feeder. Once tested, the original map feeder can be
     * replaced with the fork join version.
     */
    private final boolean useForkJoinMapFeeder;

    private final int threads;
    private final boolean enableJmx;
    private final boolean emitTaskState;
    private final SimpleDateFormat dateFormat;

    private final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

    private final AtomicBoolean metricGate = new AtomicBoolean(false);
    private final LongAdder filterTime = new LongAdder();
    private final LongAdder outputTime = new LongAdder();

    // metrics
    private static final Meter inputMeter = Metrics.newMeter(StreamMapper.class, "input", "input", TimeUnit.SECONDS);
    private static final Meter outputMeter = Metrics.newMeter(StreamMapper.class, "output", "output", TimeUnit.SECONDS);

    @GuardedBy("metricGate") private long lastTick;
    @GuardedBy("metricGate") private long lastOutputTime = 0;
    @GuardedBy("metricGate") private long lastFilterTime = 0;
    @GuardedBy("metricGate") private long lastInputCount = 0;
    @GuardedBy("metricGate") private long lastOutputCount = 0;

    private MBeanRemotingSupport jmxremote;
    private Thread feeder;

    @JsonCreator
    public StreamMapper(@JsonProperty(value = "source", required = true) TaskDataSource source,
            @JsonProperty("map") MapDef map,
            @JsonProperty(value = "output", required = true) TaskDataOutput output,
            @JsonProperty("builder") StreamBuilder builder,
            @JsonProperty("stats") boolean stats,
            @JsonProperty("metricTick") @Time(TimeUnit.NANOSECONDS) long metricTick,
            @JsonProperty(value = "threads", required = true) int threads,
            @JsonProperty("enableJmx") boolean enableJmx,
            @JsonProperty("emitTaskState") boolean emitTaskState,
            @JsonProperty("dateFormat") SimpleDateFormat dateFormat,
            @JsonProperty("validateDirs") boolean validateDirs,
            @JsonProperty("taskFinishTimeout") @Time(TimeUnit.SECONDS) int taskFinishTimeout,
            @JsonProperty("useForkJoinMapFeeder") boolean useForkJoinMapFeeder) {
        this.source = source;
        this.map = map;
        this.output = output;
        this.builder = builder;
        this.stats = stats;
        this.metricTick = metricTick;
        this.threads = threads;
        this.enableJmx = enableJmx;
        this.emitTaskState = emitTaskState;
        this.dateFormat = dateFormat;
        this.validateDirs = validateDirs;
        this.taskFinishTimeout = taskFinishTimeout;
        this.useForkJoinMapFeeder = useForkJoinMapFeeder;
        validateWritableRootPaths();
    }

    @Override
    public void start() {
        source.init();
        map.init();
        output.init();
        if (builder != null) {
            builder.init();
        }
        maybeInitJmx();
        log.info("[init]");
        if (useForkJoinMapFeeder) {
            feeder = new Thread(new MapFeederForkJoin(this, source, threads), "MapFeederForkJoin");
        } else {
            feeder = new Thread(new MapFeeder(this, source, threads), "MapFeeder");
        }
        lastTick = System.nanoTime();
        feeder.start();
    }

    public void process(Bundle inputBundle) {
        Bundle bundle = inputBundle;
        try {
            log.debug("input: {}", bundle);
            long filterBefore = System.nanoTime();
            long filterAfter;
            if ((map.filterIn == null) || map.filterIn.filter(bundle)) {
                bundle = mapBundle(bundle);
                if ((map.filterOut == null) || map.filterOut.filter(bundle)) {
                    filterAfter = System.nanoTime();
                    if (builder != null) {
                        builder.process(bundle, this);
                    } else {
                        emit(bundle);
                    }
                    long bundleOutputTime = System.nanoTime() - filterAfter;
                    outputTime.add(bundleOutputTime);
                    outputMeter.mark();
                } else {
                    filterAfter = System.nanoTime();
                    log.debug("filterOut dropped bundle : {}", bundle);
                }
            } else {
                filterAfter = System.nanoTime();
                log.debug("filterIn dropped bundle : {}", bundle);
            }
            long bundleFilterTime = filterAfter - filterBefore;
            filterTime.add(bundleFilterTime);

            // inputs are counted after outputs to prevent spurious drop reporting
            inputMeter.mark();

            // print metrics if it has been long enough
            long time = System.nanoTime();
            if (stats && ((time - lastTick) > metricTick) && metricGate.compareAndSet(false, true)) {
                // lastTick is not volatile, so double check after fencing on "metricGate"
                if ((time - lastTick) > metricTick) {
                    printMetrics(time);
                }
                metricGate.set(false);
            }
        } catch (DataChannelError ex) {
            output.sourceError(ex);
            throw ex;
        } catch (RuntimeException ex) {
            log.warn("runtime error :: {}", BundleFilterDebugPrint.formatBundle(bundle));
            output.sourceError(DataChannelError.promote(ex));
            throw ex;
        } catch (Exception ex) {
            log.warn("handling error :: {}", BundleFilterDebugPrint.formatBundle(bundle));
            DataChannelError err = DataChannelError.promote(ex);
            output.sourceError(err);
            throw err;
        }
    }

    private Bundle mapBundle(Bundle in) {
        Bundle out = output.createBundle();
        if (map.fields != null) {
            for (FieldFilter fieldFilter : map.fields) {
                fieldFilter.mapField(in, out);
            }
        } else {
            for (BundleField bundleField : in) {
                out.setValue(out.getFormat().getField(bundleField.getName()), in.getValue(bundleField));
            }
        }
        return out;
    }

    /** called directly or from builder */
    @Override public void emit(Bundle bundle) {
        log.debug("output: {}", bundle);
        output.send(bundle);
    }

    // These metrics are racey with respect to each other and the time range they cover, but no events are dropped, and
    // "extra" filtering costs for one tick will not show up in the next tick and should therefore be visible only once.
    @GuardedBy("metricGate")
    private void printMetrics(long time) {
        long nsCovered = time - lastTick;
        lastTick = time;

        double inputRateAtTick = inputMeter.oneMinuteRate();
        long inputCountAtTick = inputMeter.count();
        long inputCountForTick = inputCountAtTick - lastInputCount;
        lastInputCount = inputCountAtTick;

        double outputRateAtTick = outputMeter.oneMinuteRate();
        long outputCountAtTick = outputMeter.count();
        long outputCountForTick = outputCountAtTick - lastOutputCount;
        lastOutputCount = outputCountAtTick;

        double dropRateAtTick = Math.max(0, inputRateAtTick - outputRateAtTick);
        double dropPercentAtTick = (inputRateAtTick > 0) ? (dropRateAtTick / inputRateAtTick) : 0;
        long dropCountAtTick = Math.max(0, inputCountAtTick - outputCountAtTick);
        long dropCountForTick = Math.max(0, inputCountForTick - outputCountForTick);
        double dropPercentForTick = (double) dropCountForTick / (double) inputCountForTick;

        // filter time accounting
        long filterTimeAtTick = filterTime.sum();
        long filterTimeForTick = filterTimeAtTick - lastFilterTime;
        lastFilterTime = filterTimeAtTick;
        long filterTimePerThread = Math.min(nsCovered, filterTimeForTick / threads);

        // output time accounting
        long outputTimeAtTick = outputTime.sum();
        long outputTimeForTick = outputTimeAtTick - lastOutputTime;
        lastOutputTime = outputTimeAtTick;
        long outputTimePerThread = Math.min(nsCovered, outputTimeForTick / threads);

        // input time accounting
        long inputTimePerThread = Math.max(0, nsCovered - filterTimePerThread - outputTimePerThread);

        // ensure percentages add up to [99, 100] to avoid confusing people
        long timeDivisor = Math.max(nsCovered, filterTimePerThread + outputTimePerThread);

        double filterPercentTime = (double) filterTimePerThread / (double) timeDivisor;
        double inputPercentTime = (double) inputTimePerThread / (double) timeDivisor;
        double outputPercentTime = (double) outputTimePerThread / (double) timeDivisor;

        String countsForTick  = format("in=%,7d out=%,7d drop=%5.2f%%",
                                       inputCountForTick,
                                       outputCountForTick,
                                       dropPercentForTick * 100);
        String profileForTick = format("time {source=%3.0f%% output=%3.0f%% filters=%3.0f%%}",
                                       inputPercentTime * 100,
                                       outputPercentTime * 100,
                                       filterPercentTime * 100);
        String ratesAtTick    = format("avg {in=%,7.0f out=%,7.0f drop=%5.2f%%}",
                                       inputRateAtTick,
                                       outputRateAtTick,
                                       dropPercentAtTick * 100);
        String totalsAtTick   = format("totals {in=%,d out=%,d drop=%,d}",
                                       inputCountAtTick,
                                       outputCountAtTick,
                                       dropCountAtTick);
        StringBuilder fullMetrics =
                new StringBuilder(join(" | ", countsForTick, profileForTick, ratesAtTick, totalsAtTick));

        if (Math.abs(nsCovered - metricTick) > (metricTick / 100)) {
            fullMetrics.append(" | ")
                       .append(format("(ABNORMAL TIME SPAN) ms=%,d", TimeUnit.NANOSECONDS.toMillis(nsCovered)));
        }

        log.info(fullMetrics.toString());
    }

    @Override
    public void close() throws InterruptedException {
        feeder.interrupt();
        feeder.join();
    }

    /** called on process exit */
    public void taskComplete() {
        if (builder != null) {
            builder.streamComplete(this);
            log.info("[streamComplete] builder");
        }
        if (metricGate.compareAndSet(false, true)) {
            printMetrics(System.nanoTime());
        }
        output.sendComplete();
        emitTaskExitState();
        maybeCloseJmx();
        boolean success = completionFuture.complete(null);
        log.info("[taskComplete] Triggered future: {}", success);
    }

    /* leave artifact for minion, if desired */
    private void emitTaskExitState() {
        if (emitTaskState) {
            try {
                TaskExitState exitState = new TaskExitState();
                exitState.setInput(inputMeter.count());
                exitState.setTotalEmitted(outputMeter.count());
                exitState.setMeanRate(outputMeter.meanRate());
                Files.write(CodecJSON.INSTANCE.encode(exitState), new File("job.exit"));
            } catch (Exception ex) {
                log.error("", ex);
            }
        }
    }

    private void maybeInitJmx() {
        if (enableJmx) {
            try {
                ServerSocket ss = new ServerSocket();
                ss.setReuseAddress(true);
                ss.bind(null);
                int jmxport = ss.getLocalPort();
                ss.close();
                if (jmxport == -1) {
                    log.warn("[init.jmx] failed to get a port");
                } else {
                    try {
                        jmxremote = new MBeanRemotingSupport(jmxport);
                        jmxremote.start();
                        log.info("[init.jmx] port={}", jmxport);
                    } catch (Exception e) {
                        log.error("[init.jmx]", e);
                    }
                }
            } catch (IOException e)  {
                log.error("", e);
            }
        }
    }

    private void maybeCloseJmx() {
        if (jmxremote != null) {
            try {
                jmxremote.stop();
                jmxremote = null;
            } catch (IOException e)  {
                log.error("", e);
            }
        }
    }

    @Nonnull @Override
    public ImmutableList<Path> writableRootPaths() {
        return ImmutableList.<Path>builder()
                            .addAll(source.writableRootPaths())
                            .addAll(output.writableRootPaths())
                            .build();
    }

    public CompletableFuture<Void> getCompletionFuture() {
        return completionFuture;
    }

    public void validateWritableRootPaths() {
        if (!validateDirs) {
            return;
        }
        StringBuilder builder = new StringBuilder();
        ImmutableList<Path> sources = source.writableRootPaths();
        ImmutableList<Path> sinks = output.writableRootPaths();
        Set<Path> sourcesSet = new HashSet<>();
        Set<Path> sinksSet = new HashSet<>();
        for (Path source : sources) {
            if(!sourcesSet.add(source)) {
                String message = String.format("The writable directory is used in more than one location " +
                                               "in the input section: \"%s\"\n", source);
                builder.append(message);
            }
        }
        for (Path sink : sinks) {
            if(!sinksSet.add(sink)) {
                String message = String.format("The writable directory is used in more than one location " +
                                               "in the output section: \"%s\"\n", sink);
                builder.append(message);
            }
        }
        Sets.SetView<Path> intersect = Sets.intersection(sourcesSet, sinksSet);
        if (intersect.size() > 0) {
            String message = String.format("The following one or more directories are used in both an input " +
                                           "section and an output section: \"%s\"\n",
                                           intersect.toString());
            builder.append(message);
        }
        if (builder.length() > 0) {
            throw new IllegalArgumentException(builder.toString());
        }
    }

    public int getTaskFinishTimeout() {
        return taskFinishTimeout;
    }
}
