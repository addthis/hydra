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

import java.util.Collections;
import java.util.List;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.jackson.Jackson;
import com.addthis.metrics.reporter.config.GmondConfigParser;
import com.addthis.metrics.reporter.config.HostPort;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import info.ganglia.gmetric4j.gmetric.GMetric;
import static info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode.MULTICAST;
import info.ganglia.gmetric4j.gmetric.GMetricSlope;
import info.ganglia.gmetric4j.gmetric.GMetricType;
import info.ganglia.gmetric4j.gmetric.GangliaException;

public class GangliaOutput extends AbstractFilteredOutput {
    private static final Logger log = LoggerFactory.getLogger(GangliaOutput.class);

    private final List<HostPort> hosts;
    private final int tMax;
    private final int dMax;
    private final AutoField name;
    private final AutoField value;
    private final AutoField group;
    private final AutoField units;

    private transient List<GMetric> gmetrics;

    @JsonCreator
    public GangliaOutput(@JsonProperty("hosts") GangliaHosts hosts,
                         @JsonProperty("tMax")  int tMax,
                         @JsonProperty("dMax")  int dMax,
                         @JsonProperty("name")  AutoField name,
                         @JsonProperty("value") AutoField value,
                         @JsonProperty("group") AutoField group,
                         @JsonProperty("units") AutoField units) {
        this.hosts = hosts.gangliaHosts;
        this.tMax = tMax;
        this.dMax = dMax;
        this.name = name;
        this.value = value;
        this.group = group;
        this.units = units;
    }

    public GangliaOutput(String host,
                         int port,
                         int tMax,
                         int dMax,
                         AutoField name,
                         AutoField value,
                         AutoField group,
                         AutoField units) {
        this(new GangliaHosts(Collections.singletonList(new HostPort(host, port))),
             tMax, dMax, name, value, group, units);
    }

    /* Similar to the primary constructor, but allows directly setting the list of gmetrics. Note: do not call open. */
    @VisibleForTesting
    GangliaOutput(List<GMetric> gmetrics,
                  int tMax,
                  int dMax,
                  AutoField name,
                  AutoField value,
                  AutoField group,
                  AutoField units) {
        this(new GangliaHosts(Collections.<HostPort>emptyList()), tMax, dMax, name, value, group, units);
        this.gmetrics = gmetrics;
    }

    @Override protected void open() {
        checkState(gmetrics == null, "open was already called");
        log.info("opening ganglia output with hosts: {}", hostsToString());
        gmetrics = ImmutableList.copyOf(
                Lists.transform(hosts, new Function<HostPort, GMetric>() {
                    @Override public GMetric apply(HostPort input) {
                        try {
                            return new GMetric(input.getHost(), input.getPort(), MULTICAST, 1);
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                })
        );
    }

    private String hostsToString() {
        try {
            return Jackson.defaultMapper().writeValueAsString(hosts);
        } catch (JsonProcessingException e) {
            return e.getMessage();
        }
    }

    @Override public void send(Bundle bundle) {
        checkState(gmetrics != null, "output is either already closed or was never opened");
        if (!filter(bundle)) {
            return;
        }
        try {
            announce(name.getValue(bundle).toString(),
                     group.getValue(bundle).toString(),
                     value.getValue(bundle),
                     units.getValue(bundle).toString());
        } catch (GangliaException e) {
            throw Throwables.propagate(e);
        }
    }

    private void announce(String metricName, String metricGroup, ValueObject metricValue, String metricUnits)
            throws GangliaException {
        // see if we have to represent a long as a double
        if (metricValue.getObjectType() == ValueObject.TYPE.INT) {
            long asLong = metricValue.asLong().getLong();
            if ((int) asLong != asLong) {
                metricValue = metricValue.asDouble();
            }
        }
        for (GMetric gmetric : gmetrics) {
            gmetric.announce(metricName, metricValue.toString(), detectType(metricValue.getObjectType()),
                             metricUnits, GMetricSlope.BOTH, tMax, dMax, metricGroup);
        }
    }

    private static GMetricType detectType(ValueObject.TYPE o) {
        switch (o) {
            case FLOAT: return GMetricType.DOUBLE;
            case INT:   return GMetricType.INT32;
            default:    return GMetricType.STRING;
        }
    }

    @Override public void sendComplete() {
        checkState(gmetrics != null, "output is either already closed or was never opened");
        closeGmetrics();
    }

    // note: this method's semantics are at best unclear, so we just try to do the best was can
    @Override public void sourceError(Throwable cause) {
        log.error("source error reported; closing output in response", cause);
        if (gmetrics != null) {
            closeGmetrics();
        }
    }

    private void closeGmetrics() {
        assert gmetrics != null : "should only be called after ensuring gmetrics is not null";
        for (GMetric gmetric : gmetrics) {
            try {
                gmetric.close();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        gmetrics = null;
    }

    public static class GangliaHosts {
        public final List<HostPort> gangliaHosts;

        public GangliaHosts(String fileName) {
            this.gangliaHosts = new GmondConfigParser().getGmondSendChannels(fileName);
        }

        @JsonCreator
        public GangliaHosts(List<HostPort> gangliaHosts) {
            this.gangliaHosts = gangliaHosts;
        }
    }
}
