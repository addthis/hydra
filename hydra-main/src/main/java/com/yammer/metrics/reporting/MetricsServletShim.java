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
 *
 * This class includes modified code from the following source:
 *
 * Metrics
 * Copyright 2010-2013 Coda Hale and Yammer, Inc.
 * This product includes software developed by Coda Hale and Yammer, Inc.
 */

package com.yammer.metrics.reporting;

import javax.servlet.ServletException;

import java.io.IOException;
import java.io.Writer;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.kv.KVPairs;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.VirtualMachineMetrics;

public class MetricsServletShim extends MetricsServlet {

    private static final JsonFactory DEFAULT_JSON_FACTORY = new JsonFactory(new ObjectMapper());

    private final Clock clock;
    private final VirtualMachineMetrics vm;

    /**
     * Creates a new {@link MetricsServlet}.
     */
    public MetricsServletShim() {
        this(Clock.defaultClock(), VirtualMachineMetrics.getInstance(),
                Metrics.defaultRegistry(), DEFAULT_JSON_FACTORY, true);
    }

    public MetricsServletShim(Clock clock, VirtualMachineMetrics vm, MetricsRegistry registry,
            JsonFactory factory, boolean showJvmMetrics) {
        super(clock, vm, registry, factory, showJvmMetrics);
        this.clock = clock;
        this.vm = vm;
    }

    public void writeMetrics(Writer writer, KVPairs kv) throws ServletException, IOException {
        final String classPrefix = kv.getValue("class");
        final boolean pretty = Boolean.parseBoolean(kv.getValue("pretty"));
        final boolean showFullSamples = Boolean.parseBoolean(kv.getValue("full-samples"));

        final JsonGenerator json = DEFAULT_JSON_FACTORY.createGenerator(writer);
        if (pretty) {
            json.useDefaultPrettyPrinter();
        }
        json.writeStartObject();
        {
            if ("jvm".equals(classPrefix) || classPrefix == null) {
                writeVmMetrics(json);
            }

            writeRegularMetrics(json, classPrefix, showFullSamples);
        }
        json.writeEndObject();
        json.close();
    }

    private void writeVmMetrics(JsonGenerator json) throws IOException {
        json.writeFieldName("jvm");
        json.writeStartObject();
        {
            json.writeFieldName("vm");
            json.writeStartObject();
            {
                json.writeStringField("name", vm.name());
                json.writeStringField("version", vm.version());
            }
            json.writeEndObject();

            json.writeFieldName("memory");
            json.writeStartObject();
            {
                json.writeNumberField("totalInit", vm.totalInit());
                json.writeNumberField("totalUsed", vm.totalUsed());
                json.writeNumberField("totalMax", vm.totalMax());
                json.writeNumberField("totalCommitted", vm.totalCommitted());

                json.writeNumberField("heapInit", vm.heapInit());
                json.writeNumberField("heapUsed", vm.heapUsed());
                json.writeNumberField("heapMax", vm.heapMax());
                json.writeNumberField("heapCommitted", vm.heapCommitted());

                json.writeNumberField("heap_usage", vm.heapUsage());
                json.writeNumberField("non_heap_usage", vm.nonHeapUsage());
                json.writeFieldName("memory_pool_usages");
                json.writeStartObject();
                {
                    for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
                        json.writeNumberField(pool.getKey(), pool.getValue());
                    }
                }
                json.writeEndObject();
            }
            json.writeEndObject();

            final Map<String, VirtualMachineMetrics.BufferPoolStats> bufferPoolStats = vm.getBufferPoolStats();
            if (!bufferPoolStats.isEmpty()) {
                json.writeFieldName("buffers");
                json.writeStartObject();
                {
                    json.writeFieldName("direct");
                    json.writeStartObject();
                    {
                        json.writeNumberField("count", bufferPoolStats.get("direct").getCount());
                        json.writeNumberField("memoryUsed", bufferPoolStats.get("direct").getMemoryUsed());
                        json.writeNumberField("totalCapacity", bufferPoolStats.get("direct").getTotalCapacity());
                    }
                    json.writeEndObject();

                    json.writeFieldName("mapped");
                    json.writeStartObject();
                    {
                        json.writeNumberField("count", bufferPoolStats.get("mapped").getCount());
                        json.writeNumberField("memoryUsed", bufferPoolStats.get("mapped").getMemoryUsed());
                        json.writeNumberField("totalCapacity", bufferPoolStats.get("mapped").getTotalCapacity());
                    }
                    json.writeEndObject();
                }
                json.writeEndObject();
            }


            json.writeNumberField("daemon_thread_count", vm.daemonThreadCount());
            json.writeNumberField("thread_count", vm.threadCount());
            json.writeNumberField("current_time", clock.time());
            json.writeNumberField("uptime", vm.uptime());
            json.writeNumberField("fd_usage", vm.fileDescriptorUsage());

            json.writeFieldName("thread-states");
            json.writeStartObject();
            {
                for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages()
                        .entrySet()) {
                    json.writeNumberField(entry.getKey().toString().toLowerCase(),
                            entry.getValue());
                }
            }
            json.writeEndObject();

            json.writeFieldName("garbage-collectors");
            json.writeStartObject();
            {
                for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors()
                        .entrySet()) {
                    json.writeFieldName(entry.getKey());
                    json.writeStartObject();
                    {
                        final VirtualMachineMetrics.GarbageCollectorStats gc = entry.getValue();
                        json.writeNumberField("runs", gc.getRuns());
                        json.writeNumberField("time", gc.getTime(TimeUnit.MILLISECONDS));
                    }
                    json.writeEndObject();
                }
            }
            json.writeEndObject();
        }
        json.writeEndObject();
    }
}
