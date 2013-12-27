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
package com.addthis.hydra.task.output.tree;

import java.io.IOException;

import com.addthis.basis.jmx.FieldBasedDynamicMBean;
import com.addthis.basis.jmx.WrappingDynamicMBean;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * Encapsulation of statistics about a running hydra instance.  Registered
 * in JMX so people can look in on a Hydra remotely.
 */
public class TreeMapperStats extends WrappingDynamicMBean {

    public static final long MB = 1024L * 1024L;
    public static final ObjectMapper mapper = new ObjectMapper();

    protected Logger logger;

    public TreeMapperStats(Logger logger) {
        this.wrapped = new Snapshot();
        this.logger = logger;
    }

    public void setSnapshot(Snapshot snapshot) {
        this.wrapped = snapshot;
    }

    public static class Snapshot extends FieldBasedDynamicMBean {

        public long streamRate;
        public long streamWaitTime;
        public long mapWriteTime;
        public long localPacketRate;
        public long ruleProcessRate;
        public long nodesUpdated;
        public long totalPackets;
        public int treeCacheSize;
        public double treeCacheHitRate;
        public int treeDbCount;
        public long freeMemory;
        public String averageTimestamp;
        public long runningTime;

        public Snapshot() {
            super(true);
        }

        public String toString() {
            try {
                return mapper.writeValueAsString(this);
            } catch (IOException io) {
                return "com.addthis.hydra.task.output.tree.Snapshot:" + io.toString();
            }
        }
    }
}
