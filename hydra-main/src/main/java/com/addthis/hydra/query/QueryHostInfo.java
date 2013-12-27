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

import java.util.HashMap;
import java.util.Map;

public class QueryHostInfo {

    public enum HostState {
        CONNECTED, DISCONNECTED, WAITING, UNKNOWN
    }

    private final String uuid;
    private final String hostname;
    private final int port;
    private long lastModified;
    private boolean monitorMetrics;
    private Map<String, Object> metrics;
    private HostState state;

    public QueryHostInfo(String uuid, String hostname, int port, HostState state) {
        this.uuid = uuid;
        this.hostname = hostname;
        this.port = port;
        this.state = state;
    }

    public QueryHostInfo(String uuid, String hostname, int port) {
        this(uuid, hostname, port, HostState.UNKNOWN);
    }

    public QueryHostInfo(String uuid) {
        this(uuid, parseHostFromUUID(uuid), parsePortFromUUID(uuid), HostState.UNKNOWN);
    }

    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, Object> metrics) {
        this.metrics = metrics;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public long getLastModified() {
        return lastModified;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    public QueryHostInfo clone() {
        QueryHostInfo copy = new QueryHostInfo(this.uuid, this.hostname, this.port, this.state);
        copy.setMetrics(new HashMap<String, Object>(this.metrics));
        return copy;
    }

    public HostState getState() {
        return state;
    }

    public void setState(HostState state) {
        this.state = state;
    }

    public String getUuid() {
        return uuid;
    }

    public static String parseHostFromUUID(String uuid) {
        String uuidHost = uuid.substring(0, uuid.lastIndexOf("-"));
        if (uuid.contains("-")) {
            return uuid.substring(0, uuidHost.lastIndexOf("-"));
        } else {
            return null;
        }
    }

    public static int parsePortFromUUID(String uuid) {
        if (uuid.contains("-")) {
            String uidPort = uuid.substring(uuid.lastIndexOf("-") + 1);
            return Integer.parseInt(uidPort);
        } else {
            return -1;
        }
    }
}
