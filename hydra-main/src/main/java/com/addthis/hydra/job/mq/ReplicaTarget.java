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
package com.addthis.hydra.job.mq;

import java.io.Serializable;

import com.addthis.codec.Codec;


public class ReplicaTarget implements Codec.Codable, Serializable {

    private static final long serialVersionUID = -6611767753530890758L;

    @Codec.Set(codable = true)
    private String host;
    @Codec.Set(codable = true)
    private String hostUuid;
    @Codec.Set(codable = true)
    private String user;
    @Codec.Set(codable = true)
    private String baseDir;
    @Codec.Set(codable = true)
    private int replicationFactor;

    public ReplicaTarget() {
    }

    public ReplicaTarget(String hostUuid, String host, String user, String baseDir) {
        this(hostUuid, host, user, baseDir, 0);
    }

    public ReplicaTarget(String hostUuid, String host, String user, String baseDir, int replicationFactor) {
        this.hostUuid = hostUuid;
        this.host = host;
        this.user = user;
        this.baseDir = baseDir;
        this.replicationFactor = replicationFactor;
    }

    public String getHost() {
        return host;
    }

    public String getHostUuid() {
        return hostUuid;
    }

    public String getUser() {
        return user;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public String getUserAT() {
        return user + "@" + host;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicaTarget that = (ReplicaTarget) o;

        if (baseDir != null ? !baseDir.equals(that.baseDir) : that.baseDir != null) return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;
        if (hostUuid != null ? !hostUuid.equals(that.hostUuid) : that.hostUuid != null) return false;
        if (user != null ? !user.equals(that.user) : that.user != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + (hostUuid != null ? hostUuid.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        result = 31 * result + (baseDir != null ? baseDir.hashCode() : 0);
        return result;
    }
}
