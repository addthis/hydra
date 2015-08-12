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
package com.addthis.hydra.util;

import java.io.IOException;

import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.Time;
import com.addthis.hydra.task.output.TaskDataOutput;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.bundle.value.ValueFactory.create;

public class BundleReporter extends AbstractPollingReporter implements MetricProcessor<String>, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BundleReporter.class);

    private final TaskDataOutput output;
    private final AutoField name;
    private final AutoField value;
    private final AutoField group;
    private final AutoField units;
    private final long period;
    private final VirtualMachineMetrics vm;

    @JsonCreator
    public BundleReporter(@JsonProperty("output") TaskDataOutput output,
                          @JsonProperty("name")   AutoField name,
                          @JsonProperty("value")  AutoField value,
                          @JsonProperty("group")  AutoField group,
                          @JsonProperty("units")  AutoField units,
                          @Time(TimeUnit.NANOSECONDS)
                          @JsonProperty("period") long period) {
        this(output, name, value, group, units, period, TimeUnit.NANOSECONDS, Metrics.defaultRegistry());
    }

    public BundleReporter(TaskDataOutput output,
                          AutoField name,
                          AutoField value,
                          AutoField group,
                          AutoField units,
                          long period,
                          TimeUnit periodUnit,
                          MetricsRegistry metricsRegistry) {
        super(metricsRegistry, "bundle-reporter");
        this.output = output;
        this.name = name;
        this.value = value;
        this.group = group;
        this.units = units;
        this.vm = VirtualMachineMetrics.getInstance();
        this.period = periodUnit.toNanos(period);
    }

    public void start() {
        log.info("starting bundle metric reporter");
        this.output.init(false);
        super.start(period, TimeUnit.NANOSECONDS);
    }

    @Override public void close() throws InterruptedException {
        // would be nice if this shutdown API gave us more information, but it is just metrics so optimisim is fine
        super.shutdown(30, TimeUnit.SECONDS);
        output.sendComplete();
        log.info("bundle metric reporter closed");
    }

    @Override
    public void run() {
        printVmMetrics();
        printRegularMetrics();
    }

    private void printRegularMetrics() {
        for (Map.Entry<String, SortedMap<MetricName, Metric>> entry :
                getMetricsRegistry().groupedMetrics().entrySet()) {
            for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                final Metric metric = subEntry.getValue();
                if (metric != null) {
                    try {
                        metric.processWith(this, subEntry.getKey(), null);
                    } catch (Exception suppressed) {
                        log.error("Error printing regular metrics:", suppressed);
                    }
                }
            }
        }
    }

    private void sendMetricData(String metricName,
                                ValueObject metricValue,
                                String metricGroup,
                                String metricUnits) {
        Bundle message = output.createBundle();
        try {
            name.setValue(message, create(metricName));
            value.setValue(message, metricValue);
            group.setValue(message, create(metricGroup));
            units.setValue(message, create(metricUnits));
            output.send(message);
        } catch (Throwable t) {
            log.error("Error while trying to report metric with name {}; constructed bundle was {}",
                      metricName, message, t);
        }
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, String x) throws IOException {
        final Object value = gauge.value();
        final Class<?> klass = value.getClass();

        if (klass == Integer.class || klass == Long.class) {
            Number asNumber = (Number) gauge.value();
            sendMetricData(sanitizeName(name), create(asNumber.longValue()), "gauge", "");
        } else if (klass == Float.class || klass == Double.class) {
            Number asNumber = (Number) gauge.value();
            sendMetricData(sanitizeName(name), create(asNumber.doubleValue()), "gauge", "");
        } else {
            sendMetricData(sanitizeName(name), create(gauge.value().toString()), "gauge", "");
        }
    }

    @Override
    public void processCounter(MetricName name, Counter counter, String x) throws IOException {
        sendMetricData(sanitizeName(name), create(counter.count()), "counter", "");
    }

    @Override
    public void processMeter(MetricName name, Metered meter, String x) throws IOException {
        final String sanitizedName = sanitizeName(name);
        final String rateUnits = meter.rateUnit().name();
        final String rateUnit = rateUnits.substring(0, rateUnits.length() - 1).toLowerCase(Locale.US);
        final String unit = meter.eventType() + '/' + rateUnit;
        printLongField(sanitizedName + ".count", meter.count(), "metered", meter.eventType());
        printDoubleField(sanitizedName + ".meanRate", meter.meanRate(), "metered", unit);
        printDoubleField(sanitizedName + ".1MinuteRate", meter.oneMinuteRate(), "metered", unit);
        printDoubleField(sanitizedName + ".5MinuteRate", meter.fiveMinuteRate(), "metered", unit);
        printDoubleField(sanitizedName + ".15MinuteRate", meter.fifteenMinuteRate(), "metered", unit);
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, String x) throws IOException {
        final String sanitizedName = sanitizeName(name);
        final Snapshot snapshot = histogram.getSnapshot();
        printDoubleField(sanitizedName + ".min", histogram.min(), "histo");
        printDoubleField(sanitizedName + ".max", histogram.max(), "histo");
        printDoubleField(sanitizedName + ".mean", histogram.mean(), "histo");
        printDoubleField(sanitizedName + ".stddev", histogram.stdDev(), "histo");
        printDoubleField(sanitizedName + ".median", snapshot.getMedian(), "histo");
        printDoubleField(sanitizedName + ".75percentile", snapshot.get75thPercentile(), "histo");
        printDoubleField(sanitizedName + ".95percentile", snapshot.get95thPercentile(), "histo");
        printDoubleField(sanitizedName + ".98percentile", snapshot.get98thPercentile(), "histo");
        printDoubleField(sanitizedName + ".99percentile", snapshot.get99thPercentile(), "histo");
        printDoubleField(sanitizedName + ".999percentile", snapshot.get999thPercentile(), "histo");
    }

    @Override
    public void processTimer(MetricName name, Timer timer, String x) throws IOException {
        processMeter(name, timer, x);
        final String sanitizedName = sanitizeName(name);
        final Snapshot snapshot = timer.getSnapshot();
        final String durationUnit = timer.durationUnit().name();
        printDoubleField(sanitizedName + ".min", timer.min(), "timer", durationUnit);
        printDoubleField(sanitizedName + ".max", timer.max(), "timer", durationUnit);
        printDoubleField(sanitizedName + ".mean", timer.mean(), "timer", durationUnit);
        printDoubleField(sanitizedName + ".stddev", timer.stdDev(), "timer", durationUnit);
        printDoubleField(sanitizedName + ".median", snapshot.getMedian(), "timer", durationUnit);
        printDoubleField(sanitizedName + ".75percentile", snapshot.get75thPercentile(), "timer", durationUnit);
        printDoubleField(sanitizedName + ".95percentile", snapshot.get95thPercentile(), "timer", durationUnit);
        printDoubleField(sanitizedName + ".98percentile", snapshot.get98thPercentile(), "timer", durationUnit);
        printDoubleField(sanitizedName + ".99percentile", snapshot.get99thPercentile(), "timer", durationUnit);
        printDoubleField(sanitizedName + ".999percentile", snapshot.get999thPercentile(), "timer", durationUnit);
    }

    private static final double MIN_VAL = 1E-300;
    private void printDoubleField(String name, double value, String groupName, String units) {
        if (Math.abs(value) < MIN_VAL) value = 0.0;
        sendMetricData(name, create(value), groupName, units);
    }

    private void printDoubleField(String name, double value, String groupName) {
        printDoubleField(name, value, groupName, "");
    }

    private void printLongField(String name, long value, String groupName) {
        printLongField(name, value, groupName, "");
    }

    private void printLongField(String name, long value, String groupName, String units) {
        // TODO:  ganglia does not support int64, what should we do here?
        sendMetricData(name, create(value), groupName, units);
    }

    private void printVmMetrics() {
        printDoubleField("jvm.memory.heap_usage", vm.heapUsage(), "jvm");
        printDoubleField("jvm.memory.non_heap_usage", vm.nonHeapUsage(), "jvm");
        for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
            printDoubleField("jvm.memory.memory_pool_usages." + pool.getKey(),
                             pool.getValue(),
                             "jvm");
        }

        printDoubleField("jvm.daemon_thread_count", vm.daemonThreadCount(), "jvm");
        printDoubleField("jvm.thread_count", vm.threadCount(), "jvm");
        printDoubleField("jvm.uptime", vm.uptime(), "jvm");
        printDoubleField("jvm.fd_usage", vm.fileDescriptorUsage(), "jvm");

        for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
            printDoubleField("jvm.thread-states." + entry.getKey().toString().toLowerCase(),
                             entry.getValue(),
                             "jvm");
        }

        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
            printLongField("jvm.gc." + entry.getKey() + ".time",
                           entry.getValue().getTime(TimeUnit.MILLISECONDS),
                           "jvm");
            printLongField("jvm.gc." + entry.getKey() + ".runs", entry.getValue().getRuns(), "jvm");
        }
    }

    protected String sanitizeName(MetricName name) {
        if (name == null) {
            return "";
        }
        final String qualifiedTypeName = name.getGroup() + "." + name.getType() + "." + name.getName();
        final String metricName = name.hasScope() ? qualifiedTypeName + '.' + name.getScope() : qualifiedTypeName;
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < metricName.length(); i++) {
            final char p = metricName.charAt(i);
            if (!(p >= 'A' && p <= 'Z')
                && !(p >= 'a' && p <= 'z')
                && !(p >= '0' && p <= '9')
                && (p != '_')
                && (p != '-')
                && (p != '.')
                && (p != '\0')) {
                sb.append('_');
            } else {
                sb.append(p);
            }
        }
        return sb.toString();
    }
}
