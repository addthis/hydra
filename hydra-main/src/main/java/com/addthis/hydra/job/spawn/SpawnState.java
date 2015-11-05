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
package com.addthis.hydra.job.spawn;

import javax.annotation.Nonnull;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.codec.codables.Codable;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.util.DirectedGraph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties({"queryHost", "spawnHost", "debug", "queryPort", "quiesce"}) // ignore legacy fields
public class SpawnState implements Codable {
    private static final Logger log = LoggerFactory.getLogger(SpawnState.class);

    public final String uuid;
    @JsonProperty final AtomicInteger quiescentLevel;
    @JsonProperty final CopyOnWriteArraySet<String> disabledHosts;

    final transient ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();
    final transient DirectedGraph<String> jobDependencies = new DirectedGraph<>();

    @JsonCreator
    SpawnState(@JsonProperty("uuid") String uuid,
               @JsonProperty("quiescentLevel") AtomicInteger quiescentLevel,
               @JsonProperty("disabledHosts") CopyOnWriteArraySet<String> disabledHosts) {
        if (uuid == null) {
            this.uuid = UUID.randomUUID().toString();
            log.warn("[init] uuid was null, creating new one: {}", this.uuid);
        } else {
            this.uuid = uuid;
        }
        this.quiescentLevel = quiescentLevel;
        this.disabledHosts = disabledHosts;
    }

    @Nonnull public Iterator<Job> jobsIterator() {
        return jobs.values().iterator();
    }
}
