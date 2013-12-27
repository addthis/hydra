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
package com.addthis.hydra.query;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import java.net.URL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.basis.util.Parameter;

import com.addthis.meshy.MeshyConstants;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;
import com.addthis.meshy.service.stream.StreamSource;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * This class takes an instance of MeshyServer and continously fetches the mqworker.prop file to discover mqworkers in the mesh,
 * it then starts tracking mqworker hosts metrics. It gets the metrics by connecting by downloading the json metrics summary served by
 * the metrics http servlet that mesh query hosts have. It also provides thread safe access to host state information.
 */
//TODO: Add ability to drop, disable a host
public class MeshHostTracker {

    private static final Logger log = LoggerFactory.getLogger(MeshHostTracker.class);
    private static final String MQWORKER_METRIC_ENDPOINT = Parameter.value("mqworker.metric.endpoint", "/metrics");
    private static final String MQWORKER_PROP_FILENAME = Parameter.value("mqworker.propfile", "mqworker.prop");
    private static final long MQWORKER_SCAN_INTERVAL = Parameter.longValue("mqworker.scan.interval", 60 * 1000);
    private static final long MQWORKER_SCAN_DELAY = Parameter.longValue("mqworker.scan.interval", 1 * 1000);
    private static final String MQWORKER_LOG_DIR = Parameter.value("qworker.log.dir", "log");

    @GuardedBy("hostLock")
    //The hosts that are being tracked mapped by their mesh hostUUID
    private final Map<String, QueryHostInfo> hostMap;
    //Lock to guard the host map
    private final Lock hostLock = new ReentrantLock();
    //The meshy server where it will find and track hosts
    private final MeshyServer meshy;
    //Timer to gather the mqworker.prop file to discover new hosts
    private final Timer scanTimer;
    //A json factory to write JSON
    private static final JsonFactory factory = new JsonFactory(new ObjectMapper());

    public MeshHostTracker(MeshyServer meshy) {
        this.hostMap = new HashMap<String, QueryHostInfo>();
        this.meshy = meshy;
        this.scanTimer = new Timer("MeshHostScanTimer", true);
        this.scanTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    scanMeshForHosts();
                    fetchMetricsFromHosts();
                } catch (Exception ex) {
                    log.warn("[MeshHostTracker] Error scanning mesh for hosts info update.");
                    ex.printStackTrace();
                }
            }
        }, MQWORKER_SCAN_DELAY, MQWORKER_SCAN_INTERVAL);
    }

    /**
     * This method gathers the mqworker property file in order to discover new mqworker that have joined the mesh,
     * change those that did not respond to DISCONNECTED status, as well as remove from tracking those hosts that
     * have been in DISCONNECTED status for more than 1 minute
     *
     * @throws IOException
     */
    public void scanMeshForHosts() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("Scanning mesh for mesh query hosts");
        }
        final String mqworkerStateFile = "/" + MQWORKER_LOG_DIR + "/" + MQWORKER_PROP_FILENAME;
        final List<FileReference> fileReferences = Collections.synchronizedList(new ArrayList<FileReference>());
        final Semaphore semaphore = new Semaphore(1);
        try {
            //Gather mqworker.prop files from mesh nodes
            semaphore.acquire();
            new FileSource(meshy, MeshyConstants.LINK_NAMED, new String[]{mqworkerStateFile}) {
                @Override
                public void receiveReference(FileReference ref) {
                    if (log.isTraceEnabled()) {
                        log.trace("got mqworker properties file for: " + ref.getHostUUID());
                    }
                    fileReferences.add(ref);
                }

                @Override
                public void receiveComplete() throws Exception {
                    semaphore.release();
                }
            };
            semaphore.tryAcquire(20, TimeUnit.SECONDS);
            hostLock.lock();
            try {
                Set<String> hostUuids = new HashSet<>(hostMap.keySet());
                /**
                 *  Loop through mqworker.prop files and check if host is being tracked
                 *  From {@link Collections#synchronizedList(java.util.List)}
                 *  "It is imperative that the user manually synchronize on the returned
                 *  list when iterating over it."
                 **/
                synchronized (fileReferences) {
                    for (FileReference ref : fileReferences) {

                        QueryHostInfo host;
                        if (!hostMap.containsKey(ref.getHostUUID())) {
                            InputStream is = new StreamSource(meshy, ref.getHostUUID(), ref.name, 0).getInputStream();
                            ObjectMapper mapper = new ObjectMapper(factory);
                            JsonNode json = mapper.readTree(is);
                            host = new QueryHostInfo(ref.getHostUUID(), json.get("webHost").asText(), json.get("webPort").asInt());
                            hostMap.put(ref.getHostUUID(), host);
                        } else {
                            host = hostMap.get(ref.getHostUUID());
                        }
                        host.setState(QueryHostInfo.HostState.CONNECTED);
                        host.setLastModified(System.currentTimeMillis());
                        hostUuids.remove(host.getUuid());
                    }
                }
                for (String hostUuid : hostUuids) {
                    QueryHostInfo host = hostMap.get(hostUuid);
                    if (host.getState() == QueryHostInfo.HostState.CONNECTED) {
                        if (log.isDebugEnabled()) {
                            log.debug("Did not receive mqworker properties from host " + hostUuid + " changing state to disconnected.");
                        }
                        host.setState(QueryHostInfo.HostState.DISCONNECTED);
                        host.setLastModified(System.currentTimeMillis());
                    } else if ((System.currentTimeMillis() - host.getLastModified()) > (60 * 1000)) {
                        hostMap.remove(host.getUuid());
                    }
                }
            } finally {
                hostLock.unlock();
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for semaphore");
        }
    }

    /**
     * Loops through the hosts that are being tracked and fetches their metrics data by curling the mertrics http servlet.
     * It then updates the information for each hosts so that they have the latest metrics information.
     */
    public void fetchMetricsFromHosts() {
        if (log.isTraceEnabled()) {
            log.trace("Fetching metrics from mesh query hosts.");
        }
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        hostLock.lock();
        try {
            for (QueryHostInfo host : hostMap.values()) {
                if (host.getState() == QueryHostInfo.HostState.CONNECTED) {
                    Map<String, Object> metrics;
                    try {
                        URL hostUrl = new URL("http", host.getHostname(), host.getPort(), MQWORKER_METRIC_ENDPOINT);
                        metrics = mapper.readValue(hostUrl, typeRef);
                        host.setMetrics(metrics);
                        host.setLastModified(System.currentTimeMillis());
                        host.setState(QueryHostInfo.HostState.CONNECTED);
                    } catch (Exception ex) {
                        log.warn("Unable to fetch metrics for " + host.getHostname());
                        ex.printStackTrace();
                    }
                }
            }
        } finally {
            hostLock.unlock();
        }
    }

    /**
     * @return Returns a copy of the map of hosts keyed by mesh host UUID
     */
    public Map<String, QueryHostInfo> getHostMap() {
        Map<String, QueryHostInfo> copyMap = new HashMap<String, QueryHostInfo>();
        hostLock.lock();
        try {
            for (QueryHostInfo host : hostMap.values()) {
                QueryHostInfo copyHost = host.clone();
                copyMap.put(host.getUuid(), copyHost);
            }
        } finally {
            hostLock.unlock();
        }
        return copyMap;
    }

    /**
     * @return Returns a json string representation of the host list
     */
    public String getHostsJSON() {
        StringWriter writer = new StringWriter();
        String output;
        hostLock.lock();
        try {
            final JsonGenerator json = factory.createJsonGenerator(writer);
            json.writeStartArray();
            for (QueryHostInfo host : hostMap.values()) {
                json.writeStartObject();
                json.writeStringField("uuid", host.getUuid());
                json.writeStringField("host", host.getHostname());
                json.writeNumberField("port", host.getPort());
                json.writeNumberField("lastModified", host.getLastModified());
                json.writeNumberField("state", host.getState().ordinal());
                json.writeObjectField("metrics", host.getMetrics());
                json.writeEndObject();
            }
            json.writeEndArray();
            json.close();
        } catch (Exception ex) {
            log.warn("Error serializing json for hosts.");
            ex.printStackTrace();
        } finally {
            hostLock.unlock();
            output = writer.toString();
        }
        return output;
    }
}
