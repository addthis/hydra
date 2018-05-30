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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.Time;
import com.addthis.codec.jackson.Jackson;
import com.addthis.metrics.reporter.config.GmondConfigParser;
import com.addthis.metrics.reporter.config.HostPort;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import info.ganglia.gmetric4j.gmetric.GMetric;
import static info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode.UNICAST;
import info.ganglia.gmetric4j.gmetric.GMetricSlope;
import info.ganglia.gmetric4j.gmetric.GMetricType;
import info.ganglia.gmetric4j.gmetric.GangliaException;

public final class GangliaOutput extends AbstractFilteredOutput {
    private static final Logger log = LoggerFactory.getLogger(GangliaOutput.class);

    private final List<HostPort> hosts;
    private final int tMax;
    private final int dMax;
    private final AutoField name;
    private final AutoField value;
    private final AutoField group;
    private final AutoField units;

    @Nullable private transient List<GMetric> gmetrics;

    @JsonCreator
    private GangliaOutput(@JsonProperty("hosts") GangliaHosts hosts,
                          @JsonProperty("tMax")  @Time(TimeUnit.SECONDS) int tMax,
                          @JsonProperty("dMax")  @Time(TimeUnit.SECONDS) int dMax,
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

    @Override
    protected void open() {
        super.open();
        checkState(gmetrics == null, "open was already called");
        log.info("opening ganglia output with hosts: {}", hostsToString());
        gmetrics = hosts.stream().map(hostPort -> {
            try {
                return new GMetric(hostPort.getHost(), hostPort.getPort(), UNICAST, 1);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }).collect(Collectors.toList());
    }

    // only needed because metrics-reporter-config did not write a toString method for HostPort
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

    @Override public String toString() {
        return MoreObjects.toStringHelper(this)
                      .add("hosts", hostsToString())
                      .add("tMax", tMax)
                      .add("dMax", dMax)
                      .add("name", name)
                      .add("value", value)
                      .add("group", group)
                      .add("units", units)
                      .add("gmetrics", gmetrics)
                      .toString();
    }

    /** Exists only to support both file-name and host-port-list deserialization via a single field. */
    private static final class GangliaHosts {
        private final List<HostPort> gangliaHosts;

        @JsonCreator
        private GangliaHosts(String fileName) {
            this.gangliaHosts = new GmondConfigParser().getGmondSendChannels(fileName);
        }

        @JsonCreator
        private GangliaHosts(List<HostPort> gangliaHosts) {
            this.gangliaHosts = gangliaHosts;
        }
    }
}
