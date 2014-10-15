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

import java.util.concurrent.TimeUnit;

import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.CachingField;
import com.addthis.bundle.util.map.MapBundle;
import com.addthis.hydra.task.output.TaskDataOutput;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static com.addthis.bundle.util.map.MapBundle.decode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BundleReporterTest {
    TaskDataOutput output;
    AutoField name;
    AutoField value;
    AutoField group;
    AutoField units;
    long period;
    BundleReporter reporter;

    @Before
    public void stubBundleCreation() {
        output = mock(TaskDataOutput.class);
        name = CachingField.newAutoField("name");
        value = CachingField.newAutoField("value");
        group = CachingField.newAutoField("group");
        units = CachingField.newAutoField("units");
        period = TimeUnit.SECONDS.toNanos(60);
        reporter = new BundleReporter(output, name, value, group, units, period);
        when(output.createBundle()).thenAnswer(new Answer<Object>() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                return new MapBundle();
            }
        });
    }

    @Test
    public void reportsStringGaugeValues() throws Exception {
        reporter.processGauge(name("gauge"), gauge("value"), null);

        verify(output).send(decode("name = t.test.gauge, value = value, group = gauge, units = \"\""));
    }

    @Test
    public void escapeSlashesInMetricNames() throws Exception {
        reporter.processGauge(name("gauge_with\\slashes"), gauge("value"), null);

        verify(output).send(decode("name = t.test.gauge_with_slashes, value = value, group = gauge, units = \"\""));
    }

    @Test
    public void reportsIntegerGaugeValues() throws Exception {
        reporter.processGauge(name("gauge"), gauge(1), null);

        verify(output).send(decode("name = t.test.gauge, value = 1, group = gauge, units = \"\""));
    }

    @Test
    public void reportsLongGaugeValues() throws Exception {
        reporter.processGauge(name("gauge"), gauge(1L), null);

        verify(output).send(decode("name = t.test.gauge, value = 1, group = gauge, units = \"\""));
    }

    @Test
    public void reportsDoubleGaugeValues() throws Exception {
        reporter.processGauge(name("gauge"), gauge(1.1), null);

        verify(output).send(decode("name = t.test.gauge, value = 1.1, group = gauge, units = \"\""));
    }

    @Test
    public void reportsCounterValues() throws Exception {
        final Counter counter = mock(Counter.class);
        when(counter.count()).thenReturn(100L);

        reporter.processCounter(name("counter"), counter, null);

        verify(output).send(decode("name = t.test.counter, value = 100, group = counter, units = \"\""));
    }

    @Test
    public void reportsHistogramValues() throws Exception {
        final Histogram histogram = mock(Histogram.class);
        when(histogram.count()).thenReturn(1L);
        when(histogram.max()).thenReturn(2.0);
        when(histogram.mean()).thenReturn(3.0);
        when(histogram.min()).thenReturn(4.0);
        when(histogram.stdDev()).thenReturn(5.0);

        final Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.getMedian()).thenReturn(6.0);
        when(snapshot.get75thPercentile()).thenReturn(7.0);
        when(snapshot.get95thPercentile()).thenReturn(8.0);
        when(snapshot.get98thPercentile()).thenReturn(9.0);
        when(snapshot.get99thPercentile()).thenReturn(10.0);
        when(snapshot.get999thPercentile()).thenReturn(11.0);

        when(histogram.getSnapshot()).thenReturn(snapshot);

        reporter.processHistogram(name("histogram"), histogram, null);

        verify(output).send(decode("name = t.test.histogram.max, value = 2, group = histo, units = \"\""));
        verify(output).send(decode("name = t.test.histogram.mean, value = 3.0, group = histo, units = \"\""));
        verify(output).send(decode("name = t.test.histogram.min, value = 4, group = histo, units = \"\""));
        verify(output).send(decode("name = t.test.histogram.stddev, value = 5.0, group = histo, units = \"\""));
        verify(output).send(decode("name = t.test.histogram.median, value = 6.0, group = histo, units = \"\""));
        verify(output).send(decode("name = t.test.histogram.75percentile, value = 7.0, group = histo, units = \"\""));
        verify(output).send(decode("name = t.test.histogram.95percentile, value = 8.0, group = histo, units = \"\""));
        verify(output).send(decode("name = t.test.histogram.98percentile, value = 9.0, group = histo, units = \"\""));
        verify(output).send(decode("name = t.test.histogram.99percentile, value = 10.0, group = histo, units = \"\""));
        verify(output).send(decode("name = t.test.histogram.999percentile, value = 11.0, group = histo, units = \"\""));
    }

    @Test
    public void reportsMeterValues() throws Exception {
        final Meter meter = mock(Meter.class);
        when(meter.eventType()).thenReturn("events");
        when(meter.rateUnit()).thenReturn(TimeUnit.SECONDS);
        when(meter.count()).thenReturn(1L);
        when(meter.meanRate()).thenReturn(2.0);
        when(meter.oneMinuteRate()).thenReturn(3.0);
        when(meter.fiveMinuteRate()).thenReturn(4.0);
        when(meter.fifteenMinuteRate()).thenReturn(5.0);

        reporter.processMeter(name("meter"), meter, null);

        verify(output).send(decode("name = t.test.meter.count, value = 1, group = metered, units = events"));
        verify(output).send(decode("name = t.test.meter.meanRate, value = 2.0, group = metered, units = events/second"));
        verify(output).send(decode("name = t.test.meter.1MinuteRate, value = 3.0, group = metered, units = events/second"));
        verify(output).send(decode("name = t.test.meter.5MinuteRate, value = 4.0, group = metered, units = events/second"));
        verify(output).send(decode("name = t.test.meter.15MinuteRate, value = 5.0, group = metered, units = events/second"));
    }

    @Test
    public void reportsTimerValues() throws Exception {
        final Timer timer = mock(Timer.class);
        when(timer.eventType()).thenReturn("events");
        when(timer.durationUnit()).thenReturn(TimeUnit.MILLISECONDS);
        when(timer.rateUnit()).thenReturn(TimeUnit.SECONDS);
        when(timer.count()).thenReturn(1L);

        when(timer.meanRate()).thenReturn(2.0);
        when(timer.oneMinuteRate()).thenReturn(3.0);
        when(timer.fiveMinuteRate()).thenReturn(4.0);
        when(timer.fifteenMinuteRate()).thenReturn(5.0);
        when(timer.max()).thenReturn(2.0);
        when(timer.mean()).thenReturn(3.0);
        when(timer.min()).thenReturn(4.0);
        when(timer.stdDev()).thenReturn(5.0);

        final Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.getMedian()).thenReturn((double) 500);
        when(snapshot.get75thPercentile()).thenReturn((double) 600);
        when(snapshot.get95thPercentile()).thenReturn((double) 700);
        when(snapshot.get98thPercentile()).thenReturn((double) 800);
        when(snapshot.get99thPercentile()).thenReturn((double) 900);
        when(snapshot.get999thPercentile()).thenReturn((double) 1000);

        when(timer.getSnapshot()).thenReturn(snapshot);

        reporter.processTimer(name("another.timer"), timer, null);

        verify(output).send(decode("name = t.test.another.timer.max, value = 2.0, group = timer, units = MILLISECONDS"));
        verify(output).send(decode("name = t.test.another.timer.mean, value = 3.0, group = timer, units = MILLISECONDS"));
        verify(output).send(decode("name = t.test.another.timer.min, value = 4.0, group = timer, units = MILLISECONDS"));
        verify(output).send(decode("name = t.test.another.timer.stddev, value = 5.0, group = timer, units = MILLISECONDS"));
        verify(output).send(decode("name = t.test.another.timer.median, value = 500.0, group = timer, units = MILLISECONDS"));
        verify(output).send(decode("name = t.test.another.timer.75percentile, value = 600.0, group = timer, units = MILLISECONDS"));
        verify(output).send(decode("name = t.test.another.timer.95percentile, value = 700.0, group = timer, units = MILLISECONDS"));
        verify(output).send(decode("name = t.test.another.timer.98percentile, value = 800.0, group = timer, units = MILLISECONDS"));
        verify(output).send(decode("name = t.test.another.timer.99percentile, value = 900.0, group = timer, units = MILLISECONDS"));
        verify(output).send(decode("name = t.test.another.timer.999percentile, value = 1000.0, group = timer, units = MILLISECONDS"));

        verify(output).send(decode("name = t.test.another.timer.count, value = 1, group = metered, units = events"));
        verify(output).send(decode("name = t.test.another.timer.meanRate, value = 2.0, group = metered, units = events/second"));
        verify(output).send(decode("name = t.test.another.timer.1MinuteRate, value = 3.0, group = metered, units = events/second"));
        verify(output).send(decode("name = t.test.another.timer.5MinuteRate, value = 4.0, group = metered, units = events/second"));
        verify(output).send(decode("name = t.test.another.timer.15MinuteRate, value = 5.0, group = metered, units = events/second"));
    }

    private <T> Gauge gauge(T value) {
        final Gauge gauge = mock(Gauge.class);
        when(gauge.value()).thenReturn(value);
        return gauge;
    }

    private MetricName name(String simpleName) {
        return new MetricName("t", "test", simpleName);
    }
}