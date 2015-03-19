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
package com.addthis.hydra.task.run;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskRunConfig {

    public final int node;
    public final int nodeCount;
    public final String jobId;
    public final String dir;

    public TaskRunConfig(int node, int nodeCount, String jobid) {
        this(node, nodeCount, jobid, ".");
    }

    @JsonCreator
    public TaskRunConfig(@JsonProperty("node") int node,
                         @JsonProperty("nodeCount") int nodeCount,
                         @JsonProperty("jobId") String jobid,
                         @JsonProperty("dir") String dir) {
        this.node = node;
        this.nodeCount = nodeCount;
        this.jobId = jobid;
        this.dir = dir;
    }

    public String jobIdWithNode() {
        return jobId + "/" + node;
    }

    public Integer[] calcShardList(int shardTotal) {
        List<Integer> list = new ArrayList<>(nodeCount);
        for (int off = node; off < shardTotal; off += nodeCount) {
            list.add(off);
        }
        return list.toArray(new Integer[list.size()]);
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
                      .add("node", node)
                      .add("nodeCount", nodeCount)
                      .add("jobId", jobId)
                      .add("dir", dir)
                      .toString();
    }
}
