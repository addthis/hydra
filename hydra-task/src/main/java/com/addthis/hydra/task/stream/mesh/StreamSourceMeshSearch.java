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

package com.addthis.hydra.task.stream.mesh;

import java.io.IOException;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.hydra.data.filter.value.StringFilter;
import com.addthis.hydra.task.stream.StreamFile;
import com.addthis.hydra.task.stream.StreamSourceGrouped;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Objects.toStringHelper;
import jsr166e.ConcurrentHashMapV8;

public class StreamSourceMeshSearch implements StreamSourceGrouped {

    private static final Logger log = LoggerFactory.getLogger(StreamSourceMeshSearch.class);
    private static final Splitter HOST_METADATA_SPLITTER = Splitter.on(',').omitEmptyStrings();

    static final ConcurrentMap<String, Integer> lateFileFindMap = new ConcurrentHashMapV8<>();

    /**
     * the percentage of peers that must respond before the finder returns
     */
    private final double meshPeerThreshold;

    /**
     * if true the finder may return before 100% of peers have responded
     */
    private final boolean meshShortCircuit;

    /**
     * the amount of time the mesh finder will wait before potentially returning
     */
    private final int meshTimeOut;

    /**
     * Hostname of the meshy server. Default is either "source.mesh.host" configuration value or "localhost".
     */
    private final String meshHost;

    /**
     * Length of time to wait before possibly short circuiting mesh lookups
     */
    private int meshShortCircuitWaitTime = 5000;

    private final StringFilter pathFilter;
    private final MeshyClient meshLink;

    private volatile int peerCount = -1;

    public StreamSourceMeshSearch(MeshyClient meshLink, double meshPeerThreshold, boolean meshShortCircuit,
            int meshTimeOut, String meshHost, int meshShortCircuitWaitTime, StringFilter pathFilter) {
        this.meshLink = meshLink;
        this.meshPeerThreshold = meshPeerThreshold;
        this.meshShortCircuit = meshShortCircuit;
        this.meshTimeOut = meshTimeOut;
        this.meshHost = meshHost;
        this.meshShortCircuitWaitTime = meshShortCircuitWaitTime;
        this.pathFilter = pathFilter;
    }

    /**
     * query the mesh for a list of matching files
     */
    public List<MeshyStreamFile> findMeshFiles(String... patterns) throws IOException {
        log.trace("find using mesh={} patterns={}", meshLink, patterns);
        final AtomicInteger respondingPeerCount = new AtomicInteger();
        final Semaphore gate = new Semaphore(0);
        final ConcurrentHashMap<String, Histogram> responseTimeMap = new ConcurrentHashMap<>();

        final List<MeshyStreamFile> fileReferences = new ArrayList<>();
        final long startTime = System.currentTimeMillis();
        final AtomicBoolean shortCircuited = new AtomicBoolean(false);
        peerCount = -1;
        FileSource source = new FileSource(meshLink, patterns, "localF") {
            // both must be initialized in 'peers' to make sense elsewhere; TODO: add explicit state
            boolean localMeshFindRunning;
            Set<String> unfinishedHosts; // purely cosmetic

            @Override
            public void receiveReference(FileReference ref) {
                String name = ref.name;
                if (name.charAt(0) != '/') {
                    switch (name) {
                        case "peers":
                            // the host uuid is a list of remote peers who were sent requests
                            unfinishedHosts =
                                    Sets.newHashSet(HOST_METADATA_SPLITTER.split(ref.getHostUUID()));
                            unfinishedHosts.add(meshHost);
                            // include the local mesh node
                            peerCount = (int) ref.size + 1;
                            localMeshFindRunning = true;
                            StreamSourceMeshSearch.log.debug(toLogString("init"));
                            return;
                        case "response":
                            unfinishedHosts.remove(ref.getHostUUID());
                            // ref.size is the number of outstanding remote requests
                            int outstandingRequests = (int) ref.size;
                            // adjust for a possibly outstanding local request
                            if (localMeshFindRunning) {
                                outstandingRequests += 1;
                            }
                            int newCompleteResponsesCount = peerCount - outstandingRequests;
                            // information is allowed to be forwarded out of order so take maximum
                            respondingPeerCount.set(Math.max(newCompleteResponsesCount, respondingPeerCount.get()));
                            StreamSourceMeshSearch.log.debug(toLogString("response"));
                            return;
                        case "localfind":
                            localMeshFindRunning = false;
                            unfinishedHosts.remove(meshHost);
                            respondingPeerCount.incrementAndGet();
                            StreamSourceMeshSearch.log.debug(toLogString("localfind"));
                            return;
                        default:
                            StreamSourceMeshSearch.log.warn("Found a file ref without a prepended /. Assuming its a real fileref for now : {}", ref.name);
                    }
                }
                String hostId = ref.getHostUUID().substring(0, ref.getHostUUID().indexOf("-"));
                if (shortCircuited.get()) {
                    int lateCount = (lateFileFindMap.get(hostId) == null ? 1 : lateFileFindMap.get(hostId));
                    lateFileFindMap.put(hostId, lateCount + 1);
                    // we are done here
                    return;
                }
                long receiveTime = System.currentTimeMillis();
                fileReferences.add(new MeshyStreamFile(ref, meshLink, pathFilter));
                if (responseTimeMap.containsKey(hostId)) {
                    Histogram histo = responseTimeMap.get(hostId);
                    histo.update(receiveTime - startTime);
                } else {
                    Histogram histo = Metrics.newHistogram(StreamSourceMeshPipe.class, hostId + "refResponseTime.JMXONLY");
                    responseTimeMap.put(hostId, histo);
                    histo.update(receiveTime - startTime);
                }
            }

            // called when all mesh nodes have completed. In this case we have only one mesh node who should only
            //   trigger this event when all the remote mesh nodes have completed
            @Override
            public void receiveComplete() throws Exception {
                StreamSourceMeshSearch.log.debug(toLogString("all-complete"));
                gate.release();
            }

            @Override
            public String toString() {
                return toLogString("FileRefSource");
            }

            private String toLogString(String reason) {
                return toStringHelper(reason)
                        .add("peer-count", peerCount)
                        .add("responses", respondingPeerCount.get())
                        .add("run-time", System.currentTimeMillis() - startTime)
                        .add("waiting", unfinishedHosts)
                        .toString();
            }
        };
        while (true) {
            try {
                if (!gate.tryAcquire(meshShortCircuitWaitTime, TimeUnit.MILLISECONDS)) {
                    if (meshShortCircuit
                        && peerCount > 0
                        && (System.currentTimeMillis() - startTime) > meshTimeOut
                        && respondingPeerCount.get() > (meshPeerThreshold * peerCount)) {
                        // break early
                        shortCircuited.set(true);
                        log.warn("Breaking after receiving responses from {} of {} peers",
                                respondingPeerCount.get(), peerCount);
                        break;
                    } else {
                        try {
                            log.warn(source.toString());
                        } catch (ConcurrentModificationException cme) {
                            //then we must be at least making progress so whatever
                        }
                    }
                } else {
                    // got the lock, all set!
                    break;
                }
            } catch (InterruptedException ignored) {
            }
        }
        if (log.isTraceEnabled()) {
            double max = -1.0d;
            String slowHost = "";
            for (Map.Entry<String, Histogram> entry : responseTimeMap.entrySet()) {
                log.trace("hostTime: " + entry.getKey() + " - " + entry.getValue().max());
                if (entry.getValue().max() > max) {
                    slowHost = entry.getKey() + " - " + entry.getValue().max();
                    max = entry.getValue().max();
                }
            }
            log.trace("\nslowHost: " + slowHost);
        }
        return fileReferences;
    }

    @Override
    public Set<StreamFile> nextGroup() {
        return null;
    }
}
