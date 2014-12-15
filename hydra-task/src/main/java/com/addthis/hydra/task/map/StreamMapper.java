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

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;

import java.net.ServerSocket;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;

import com.addthis.basis.jmx.MBeanRemotingSupport;
import com.addthis.basis.util.JitterClock;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.data.filter.bundle.BundleFilterDebugPrint;
import com.addthis.hydra.data.util.TimeField;
import com.addthis.hydra.task.output.TaskDataOutput;
import com.addthis.hydra.task.run.TaskExitState;
import com.addthis.hydra.task.run.TaskRunnable;
import com.addthis.hydra.task.source.TaskDataSource;

import com.google.common.io.Files;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <pre>{
 *    type : "map",
 * <p/>
 *     source : {
 *         ...
 *     },
 * <p/>
 *     map : {
 *         ...
 *     },
 * <p/>
 *     output : {
 *         ...
 *     },
 * }</pre>
 *
 * @user-reference
 * @hydra-name map
 */
public class StreamMapper implements StreamEmitter, TaskRunnable {
    private static final Logger log = LoggerFactory.getLogger(StreamMapper.class);

    private static final NumberFormat percentFormat = NumberFormat.getPercentInstance();

    /** The data source for this job. */
    @JsonProperty(required = true) private TaskDataSource source;

    /** The transformations to apply onto the data. */
    @JsonProperty private MapDef map;

    /** The data sink for emitting the result of the transformations. */
    @JsonProperty(required = true) private TaskDataOutput output;

    /**
     * Allow more flexible stream builders.  For example, asynchronous or builders
     * where one or more bundles roll up into a single bundle or a single bundle causes
     * the emission of multiple bundles.
     */
    @JsonProperty private StreamBuilder builder;

    /** Print to the console statistics while processing the data. Default is {@code true}. */
    @JsonProperty private boolean stats = true;

    /** Optionally extract bundle time and print average of bundles processed since last log line */
    @JsonProperty private TimeField timeField;

    @JsonProperty(required = true) private int threads;
    @JsonProperty private boolean enableJmx;
    @JsonProperty private boolean emitTaskState;
    @JsonProperty private SimpleDateFormat dateFormat;

    private final AtomicLong totalEmit = new AtomicLong(0);
    private final AtomicLong bundleTimeSum = new AtomicLong(0);
    private final LongAdder filterTime = new LongAdder();
    private final AtomicBoolean emitGate = new AtomicBoolean(false);

    @GuardedBy("emitGate")
    private long lastFilterTime = 0;

    private MBeanRemotingSupport jmxremote;
    private long lastMark;
    private Thread feeder;

    // metrics
    private final Meter   processedMeterMetric  =
            Metrics.newMeter(getClass(), "streamMapper", "processedMeter", TimeUnit.SECONDS);
    private final Counter inputCountMetric      = Metrics.newCounter(getClass(), "inputCount");
    private final Counter outputCountMetric     = Metrics.newCounter(getClass(), "outputCount");
    private final Counter totalInputCountMetric = Metrics.newCounter(getClass(), "totalInputCount");

    @Override
    public void start() {
        if (output == null) {
            throw new RuntimeException("missing output definition");
        }
        if (map == null) {
            map = new MapDef();
        }
        source.init();
        output.init();
        if (builder != null) {
            builder.init();
        }
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
        log.info("[init]");
        feeder = new Thread(new MapFeeder(this, source, threads),"MapFeeder");
        feeder.start();
    }

    @Override
    public void close() throws InterruptedException {
        feeder.interrupt();
        feeder.join();
    }

    private Bundle mapBundle(Bundle in) {
        if (map.fields == null) {
            Bundle out = output.createBundle();
            for (BundleField bundleField : in) {
                out.setValue(out.getFormat().getField(bundleField.getName()), in.getValue(bundleField));
            }
            return out;
        }
        Bundle out = output.createBundle();
        for (FieldFilter fieldFilter : map.fields) {
            fieldFilter.mapField(in, out);
        }
        return out;
    }

    /** called directly or from builder */
    @Override public void emit(Bundle bundle) {
        log.debug("output: {}", bundle);
        output.send(bundle);
        outputCountMetric.inc();
        processedMeterMetric.mark();
        totalEmit.incrementAndGet();
        if (timeField != null) {
            bundleTimeSum.addAndGet(getBundleTime(bundle) >> 8);
        }
    }

    public void process(Bundle bundle) {
        try {
            log.debug("input: {}", bundle);
            inputCountMetric.inc();
            totalInputCountMetric.inc();
            long markBefore = System.nanoTime();
            long markAfter;
            if ((map.filterIn == null) || map.filterIn.filter(bundle)) {
                bundle = mapBundle(bundle);
                if ((map.filterOut == null) || map.filterOut.filter(bundle)) {
                    markAfter = System.nanoTime();
                    if (builder != null) {
                        builder.process(bundle, this);
                    } else {
                        emit(bundle);
                    }
                } else {
                    markAfter = System.nanoTime();
                    log.debug("filterOut dropped bundle : {}", bundle);
                }
            } else {
                markAfter = System.nanoTime();
                log.debug("filterIn dropped bundle : {}", bundle);
            }
            long bundleFilterTime = markAfter - markBefore;
            filterTime.add(bundleFilterTime);
            long time = JitterClock.globalTime();
            if (((time - lastMark) > 1000) && emitGate.compareAndSet(false, true)) {
                long in = inputCountMetric.count();
                inputCountMetric.clear();
                long out = outputCountMetric.count();
                outputCountMetric.clear();
                long msCovered = time - lastMark;

                // filter time accounting
                long filterTimeAtTick = filterTime.sum();
                long filterTimeForTick = filterTimeAtTick - lastFilterTime;
                lastFilterTime = filterTimeAtTick;
                long filterTimePerThread = filterTimeForTick / threads;
                double filterPercentTime =
                        (double) TimeUnit.NANOSECONDS.toMillis(filterTimePerThread) / (double) msCovered;
                String filterPercentTimeFormatted = percentFormat.format(filterPercentTime);

                if (stats) {
                    if (timeField != null) {
                        long avg_t = (bundleTimeSum.getAndSet(0) / Math.max(1,out)) << 8;
                        log.info("bundleTime={} in={} out={} drop={} ms={} filtering={} rate={} total={}",
                                 dateFormat.format(avg_t),
                                 in, out, Math.max(in - out, 0), msCovered,
                                 filterPercentTimeFormatted,
                                 Math.round(processedMeterMetric.oneMinuteRate()),
                                 totalEmit);
                    } else {
                        log.info("in={} out={} drop={} ms={} filtering={} rate={} total={}",
                                 in, out, Math.max(in - out, 0), msCovered,
                                 filterPercentTimeFormatted,
                                 Math.round(processedMeterMetric.oneMinuteRate()),
                                 totalEmit);
                    }
                }
                lastMark = time;
                emitGate.set(false);
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

    /* leave artifact for minion, if desired */
    private void emitTaskExitState() {
        if (emitTaskState) {
            try {
                TaskExitState exitState = new TaskExitState();
                exitState.setInput(totalInputCountMetric.count());
                exitState.setTotalEmitted(totalEmit.get());
                exitState.setMeanRate(processedMeterMetric.meanRate());
                Files.write(CodecJSON.INSTANCE.encode(exitState), new File("job.exit"));
            } catch (Exception ex) {
                log.error("", ex);
            }
        }
    }

    /* borrowed from TreeMapper.java */
    private long getBundleTime(Bundle bundle) {
        long bundleTime = JitterClock.globalTime();
        ValueObject vo = bundle.getValue(bundle.getFormat().getField(timeField.getField()));
        if (vo == null) {
            log.debug("missing time {} in [{}] --> {}", timeField.getField(), bundle.getCount(), bundle);
        } else {
            bundleTime = timeField.toUnix(vo);
        }
        return bundleTime;
    }

    /** called on process exit */
    public void taskComplete() {
        if (builder != null) {
            builder.streamComplete(this);
            log.info("[streamComplete] builder");
        }
        output.sendComplete();
        emitTaskExitState();
        if (jmxremote != null) {
            try {
                jmxremote.stop();
                jmxremote = null;
            } catch (IOException e)  {
                log.error("", e);
            }
        }
        log.info("[taskComplete]");
    }
}
