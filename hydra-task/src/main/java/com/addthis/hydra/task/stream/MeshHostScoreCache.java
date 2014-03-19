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
package com.addthis.hydra.task.stream;

import java.io.ByteArrayInputStream;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;

import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.stream.SourceInputStream;
import com.addthis.meshy.service.stream.StreamSource;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around a guava loading cache for mesh host stats. The current stat implementation is just the number of
 * open files that a mesh node has (as provided by the meshy stats virtual file end point).
 * <p/>
 */
public class MeshHostScoreCache {

    private static final Logger log = LoggerFactory.getLogger(MeshHostScoreCache.class);

    // Time to wait before refreshing the meshy stats of a mesh node (in milliseconds). Is not done in the background
    private static final int refreshTime = Parameter.intValue("source.mesh.score.refresh", 10000);

    // Time to wait before giving up on a score assessment and making up something bad (in milliseconds)
    private static final int pollTime = Parameter.intValue("source.mesh.score.poll", 1000);

    // Amount to add to the score of each mesh node. Effectively says they have x more open files than they really do.
    // Increases randomness among low file counts.
    private static final int scoreFudge = Parameter.intValue("source.mesh.score.fudge", 2);

    private static final Meter loadProblems = Metrics.newMeter(MeshHostScoreCache.class, "loadProblems", "loadProblems",
            TimeUnit.SECONDS);

    private final LoadingCache<String, Integer> meshCache;

    public MeshHostScoreCache(final MeshyClient meshLink) {
        meshCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(refreshTime, TimeUnit.MILLISECONDS)
                .build(
                        new CacheLoader<String, Integer>() {
                            public Integer load(String host) throws Exception {
                                try (SourceInputStream meshSource
                                             = new StreamSource(meshLink, host, "/meshy/statsMap", 0).getInputStream()) {
                                    byte[] response = meshSource.poll(pollTime, TimeUnit.MILLISECONDS);
                                    if (response == null) {
                                        loadProblems.mark();
                                        return 50;
                                    }
                                    ByteArrayInputStream in = new ByteArrayInputStream(response);
                                    int count = Bytes.readInt(in);
                                    HashMap<String, Integer> stats = new HashMap<>(1);
                                    while (count-- > 0) {
                                        String key = Bytes.readString(in);
                                        Integer val = Bytes.readInt(in);
                                        stats.put(key, val);
                                    }
                                    return stats.get("sO") + scoreFudge;
                                }
                            }
                        });
    }

    public int get(String host) {
        try {
            return meshCache.get(host);
        } catch (Exception e) {
            log.debug("", e);
            // If an unexpected error occurs, say that the host has about 50 open files as a safe case
            loadProblems.mark();
            return 50;
        }
    }
}
