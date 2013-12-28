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

import java.io.File;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import java.text.DecimalFormat;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.data.filter.bundle.BundleFilterDebugPrint;
import com.addthis.hydra.data.filter.value.ValueFilter;
import com.addthis.hydra.task.output.TaskDataOutput;
import com.addthis.hydra.task.run.TaskExitState;
import com.addthis.hydra.task.run.TaskFeeder;
import com.addthis.hydra.task.run.TaskRunConfig;
import com.addthis.hydra.task.run.TaskRunTarget;
import com.addthis.hydra.task.run.TaskRunnable;
import com.addthis.hydra.task.source.TaskDataSource;

import com.google.common.io.Files;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * <p>This is <span class="hydra-summary">the most common form of Hydra job (either a split job or a map job)</span>. It is specified with
 * <code>type : "map"</code>.</p>
 * <p>There are two common use cases of these jobs:</p>
 * <ul>
 * <li><p>Split jobs. These jobs take in lines of data, such as log files, and emit new lines.
 * It might change the format (text in, binary out),
 * or drop lines that fail to some predicate, or create multiple derived lines from each input,
 * or make all strings lowercase, or other arbitrary transformations.
 * But it's always lines in, lines out.</li>
 * <li>TreeBuilder (or Map) jobs. These jobs take in log lines of input,
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
public class StreamMapper extends TaskRunnable implements StreamEmitter, TaskRunTarget {

    private final Logger log = LoggerFactory.getLogger(StreamMapper.class);
    private final boolean emitTaskState = Parameter.boolValue("task.mapper.emitState", true);

    /**
     * The data source for this job.
     */
    @Codec.Set(codable = true, required = true)
    private TaskDataSource source;
    /**
     * The transformations to apply onto the data.
     */
    @Codec.Set(codable = true)
    private MapDef map;
    /**
     * The data sink for emitting the result of the transformations.
     */
    @Codec.Set(codable = true, required = true)
    private TaskDataOutput output;
    /**
     * Lorem ipsum dolor sit amet.
     */
    @Codec.Set(codable = true)
    private StreamBuilder builder;
    /**
     * Print to the console statistics while processing the data. Default is <code>true</code>.
     */
    @Codec.Set(codable = true)
    private boolean stats = true;

    private final AtomicLong totalEmit = new AtomicLong(0);
    private final AtomicBoolean emitGate = new AtomicBoolean(false);
    private final long startTime = JitterClock.globalTime();
    private long lastMark;
    private TaskRunConfig config;
    private TaskFeeder feeder;

    // metrics
    private final Meter processedMeterMetric = Metrics.newMeter(getClass(), "streamMapper", "processedMeter", TimeUnit.SECONDS);
    private final Counter inputCountMetric = Metrics.newCounter(getClass(), "inputCount");
    private final Counter outputCountMetric = Metrics.newCounter(getClass(), "outputCount");
    private final Counter totalInputCountMetric = Metrics.newCounter(getClass(), "totalInputCount");
    private final AtomicLong bundleFilterTime = new AtomicLong(0);

    /**
     * This section defines the transformations to apply onto the data.
     * <p/>
     * <p>The {@link #fields fields} section defines how the fields of the input source
     * are transformed into a mapped bundle. The {@link #filterIn filterIn} filter is applied
     * before the fields transformation. The {@link #filterOut filterOut} filter is applied
     * after the fields transformation. filterIn can be used to improve job performance
     * by eliminating unneeded records so that they do not need to be transformed.</p>
     * <p/>
     * <p>To specify a series of filters for the filterIn or filterOut use
     * a {@link com.addthis.hydra.data.filter.bundle.BundleFilterChain chain} bundle filter.</p>
     * <p/>
     * <p>Example:</p>
     * <pre>map:{
     *    filterIn: {op:"chain", filter:[
     *       {op:"field", from:"TIME", filter:{op:"chain", filter:[
     *          {op:"empty", not:true},
     *          {op:"require", match:["[0-9]{13}"]},
     *       ]}},
     *    ]},
     *    fields:[
     *       {from:"TIME", to:"TIME"},
     *       {from:"SOURCE", to:"SOURCE"},
     *       {from:"QUERY_PARAMS"},
     *    ],
     *    filterOut:{op:"chain", filter:[
     *       {op:"time", src:{field:"TIME", format:"native"},
     *             dst:{field:"DATE", format:"yyMMdd-HHmmss", timeZone:"America/New_York"}},
     *       {op:"field", from:"DATE", to:"DATE_YMD", filter:{op:"slice", to:6}},
     *    ]}
     * }</pre>
     *
     * @user-reference
     */
    public static final class MapDef implements Codec.Codable {

        /**
         * The filter to apply before field transformation.
         */
        @Codec.Set(codable = true)
        private BundleFilter filterIn;

        /**
         * The filter to apply after field transformation.
         */
        @Codec.Set(codable = true)
        private BundleFilter filterOut;

        /**
         * The mapping of fields from the input source into the bundle.
         */
        @Codec.Set(codable = true)
        private FieldFilter[] fields;
    }

    /**
     * This section specifies how fields of the input source are transformed into a mapped bundle.
     * <p/>
     * <p>Fields are moved from a specified field in the job {@link StreamMapper#source source}
     * to a destination field in the mapped bundle. If the 'to' field is not specified then
     * the destination field is assumed to have the same name as the 'from' field. By default
     * null values are not written into the mapped bundle. This behavior can be changed by
     * setting the toNull field to true.</p>
     * <p/>
     * <p>Example:</p>
     * <pre>fields:[
     *    {from:"TIME", to:"TIME"},
     *    {from:"SOURCE", to:"SOURCE"},
     *    {from:"QUERY_PARAMS"},
     * ]</pre>
     *
     * @user-reference
     */
    public static final class FieldFilter implements Codec.Codable {

        /**
         * The name of the bundle field source. This is required.
         */
        @Codec.Set(codable = true, required = true)
        private String from;

        /**
         * The name of the bundle field destination. If not specified then use the 'from' field.
         */
        @Codec.Set(codable = true)
        private String to;

        /**
         * Optionally apply a filter onto the field.
         */
        @Codec.Set(codable = true)
        private ValueFilter filter;

        /**
         * If true then emit null values to the destination field. The default is false.
         */
        @Codec.Set(codable = true)
        private boolean toNull;
    }

    @Override
    public void init(TaskRunConfig config) {
        if (getOutput() == null) {
            throw new RuntimeException("missing output definition");
        }
        if (map == null) {
            map = new MapDef();
        }
        getSource().init(config);
        getOutput().init(config);
        if (builder != null) {
            builder.init();
        }
        this.config = config;
        log.info("[init] " + config);
    }

    @Override
    public void exec() {
        feeder = new TaskFeeder(this, config.getThreadCount());
    }

    @Override
    public void terminate() {
        feeder.terminate();
    }

    @Override
    public void waitExit() {
        feeder.waitExit();
    }

    @Override
    public TaskDataSource getSource() {
        return source;
    }

    public TaskDataOutput getOutput() {
        return output;
    }

    private Bundle mapBundle(Bundle in) {
        if (map.fields == null) {
            return in;
        }
        Bundle out = getOutput().createBundle();
        for (int i = 0; i < map.fields.length; i++) {
            ValueObject inVal = in.getValue(in.getFormat().getField(map.fields[i].from));
            if (map.fields[i].filter != null) {
                inVal = map.fields[i].filter.filter(inVal);
            }
            if (inVal != null || map.fields[i].toNull) {
                out.setValue(out.getFormat().getField(map.fields[i].to), inVal);
            }
        }
        return out;
    }

    /**
     * called directly or from builder
     */
    public void emit(Bundle bundle) {
        log.debug("output: " + bundle);
        getOutput().send(bundle);
        outputCountMetric.inc();
        totalEmit.incrementAndGet();
    }

    @Override
    public void process(Bundle bundle) {
        try {
            log.debug("input: " + bundle);
            inputCountMetric.inc();
            totalInputCountMetric.inc();
            processedMeterMetric.mark();
            long markBefore = System.nanoTime();
            if (map.filterIn == null || map.filterIn.filter(bundle)) {
                bundle = mapBundle(bundle);
                if (map.filterOut == null || map.filterOut.filter(bundle)) {
                    bundleFilterTime.addAndGet(System.nanoTime() - markBefore);
                    if (builder != null) {
                        builder.process(bundle, this);
                    } else {
                        emit(bundle);
                    }
                } else {
                    bundleFilterTime.addAndGet(System.nanoTime() - markBefore);
                    log.debug("filterOut dropped bundle : " + bundle);
                }
            } else {
                bundleFilterTime.addAndGet(System.nanoTime() - markBefore);
                log.debug("filterIn dropped bundle : " + bundle);
            }
            long time = JitterClock.globalTime();
            if (time - lastMark > 1000 && emitGate.compareAndSet(false, true)) {
                long in = inputCountMetric.count();
                inputCountMetric.clear();
                long out = outputCountMetric.count();
                outputCountMetric.clear();
                if (stats) {
                    log.info("run=" + (System.currentTimeMillis() - startTime) +
                             " in=" + in + " out=" + out + " skip=" + Math.max(in - out, 0) + " ms=" + (time - lastMark) +
                             " rate=" + Math.round(processedMeterMetric.oneMinuteRate()) +
                             " total=" + totalEmit + " filterTime=" + pad(bundleFilterTime.getAndSet(0), 6));
                }
                lastMark = time;
                emitGate.set(false);
            }
        } catch (DataChannelError ex) {
            getOutput().sourceError(ex);
            throw ex;
        } catch (RuntimeException ex) {
            log.warn("runtime error :: " + BundleFilterDebugPrint.formatBundle(bundle));
            getOutput().sourceError(DataChannelError.promote(ex));
            throw ex;
        } catch (Exception ex) {
            log.warn("handling error :: " + BundleFilterDebugPrint.formatBundle(bundle));
            DataChannelError err = DataChannelError.promote(ex);
            getOutput().sourceError(err);
            throw err;
        }
    }

    /* leave artifact for minion, if desired */
    private void emitTaskExitState() {
        if (emitTaskState) {
            try {
                TaskExitState exitState = new TaskExitState();
                exitState.setHadMoreData(source.hadMoreData());
                exitState.setInput(totalInputCountMetric.count());
                exitState.setTotalEmitted(totalEmit.get());
                exitState.setMeanRate(processedMeterMetric.meanRate());
                Files.write(new CodecJSON().encode(exitState), new File("job.exit"));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * number right pad utility for log data
     */
    private static String pad(long v, int chars) {
        String sv = Long.toString(v);
        String opt[] = new String[]{"K", "M", "B", "T"};
        DecimalFormat dco[] = new DecimalFormat[]{new DecimalFormat("0.00"), new DecimalFormat("0.0"), new DecimalFormat("0")};
        int indx = 0;
        double div = 1000d;
        outer:
        while (sv.length() > chars - 1 && indx < opt.length) {
            for (DecimalFormat dc : dco) {
                sv = dc.format(v / div).concat(opt[indx]);
                if (sv.length() <= chars - 1) {
                    break outer;
                }
            }
            div *= 1000;
            indx++;
        }
        return Strings.padright(sv, chars);
    }

    @Override
    public void taskComplete() {
        if (builder != null) {
            builder.streamComplete(this);
            log.info("[streamComplete] builder");
        }
        getOutput().sendComplete();
        log.info("[taskComplete]");
        emitTaskExitState();
    }
}
