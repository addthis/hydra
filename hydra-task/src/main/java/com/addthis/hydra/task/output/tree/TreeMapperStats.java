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

import java.text.DecimalFormat;

import com.addthis.basis.jmx.FieldBasedDynamicMBean;
import com.addthis.basis.jmx.WrappingDynamicMBean;

import com.google.common.base.Strings;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Encapsulation of statistics about a running hydra instance.  Registered
 * in JMX so people can look in on a Hydra remotely.
 */
public class TreeMapperStats extends WrappingDynamicMBean {

    public static final long MB = 1024L * 1024L;
    private static final DecimalFormat percent = new DecimalFormat("00.0%");
    public static final ObjectMapper mapper = new ObjectMapper();

    public TreeMapperStats() {
        this.wrapped = new Snapshot();
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

        public String toFormattedString() {
            StringBuilder msg = new StringBuilder();
            msg.append(pad(streamWaitTime, 6));
            msg.append(pad(mapWriteTime, 6));
            msg.append(pad(streamRate, 6));
            msg.append(pad(localPacketRate, 6));
            msg.append(pad(ruleProcessRate, 7));
            msg.append(pad(nodesUpdated, 6));
            msg.append(pad(totalPackets, 8));
            msg.append(pad(treeCacheSize, 6));
            msg.append(Strings.padEnd(percent.format(treeCacheHitRate), 6, ' '));
            msg.append(pad(treeDbCount, 6));
            msg.append(pad(freeMemory, 6));
            msg.append(Strings.padEnd(averageTimestamp, 14, ' '));
            return msg.toString();
        }

        /** number right pad utility for log data */
        private static String pad(long v, int chars) {
            String sv = Long.toString(v);
            String[] opt = {"K", "M", "B", "T"};
            DecimalFormat[] dco = {new DecimalFormat("0.00"), new DecimalFormat("0.0"), new DecimalFormat("0")};
            int indx = 0;
            double div = 1000.0d;
            outer:
            while ((sv.length() > (chars - 1)) && (indx < opt.length)) {
                for (DecimalFormat dc : dco) {
                    sv = dc.format(v / div).concat(opt[indx]);
                    if (sv.length() <= (chars - 1)) {
                        break outer;
                    }
                }
                div *= 1000;
                indx++;
            }
            return Strings.padEnd(sv, chars, ' ');
        }

    }
}
