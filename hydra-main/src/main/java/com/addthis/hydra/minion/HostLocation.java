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
package com.addthis.hydra.minion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds physical location info of a minion. The info can be useful to decide where the replicas should be put, e.g.
 * replicas should be spread across different datacenters as much as possible, to minimize the risk of data loss
 */

public class HostLocation {
    private final String dataCenter;
    private final String rack;
    private final String physicalHost; // a physical host can have many VMs

    @JsonCreator
    HostLocation(@JsonProperty("dataCenter") String dataCenter,
                 @JsonProperty("rack") String rack,
                 @JsonProperty("physicalHost") String physicalHost) {
        this.dataCenter = dataCenter;
        this.rack = rack;
        this.physicalHost = physicalHost;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public String getRack() {
        return rack;
    }

    public String getPhysicalHost() {
        return physicalHost;
    }

    public String toString() {
        return "dataCenter=" + dataCenter + ", rack=" + rack + ", physicalHost=" + physicalHost;
    }
}
