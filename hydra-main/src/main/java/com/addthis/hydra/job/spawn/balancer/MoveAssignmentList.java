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
package com.addthis.hydra.job.spawn.balancer;

import java.util.ArrayList;
import java.util.List;

import com.addthis.hydra.job.JobTaskMoveAssignment;
import com.addthis.hydra.job.spawn.Spawn;

class MoveAssignmentList {
    private List<JobTaskMoveAssignment> moveAssignmentList;
    private static final long serialVersionUID = -563719566151798849L;

    private final Spawn spawn;
    private final SpawnBalancerTaskSizer taskSizer;

    private long bytesUsed = 0;

    public MoveAssignmentList(Spawn spawn, SpawnBalancerTaskSizer taskSizer) {
        this.spawn = spawn;
        this.taskSizer = taskSizer;
        this.moveAssignmentList = new ArrayList<>();
    }

    public int size() {
        return this.moveAssignmentList.size();
    }

    public boolean isEmpty() {
        return this.moveAssignmentList.isEmpty();
    }

    /**
     * Returns <tt>true</tt> if the list contains a move assignment
     * to move any task associated with the specified job.
     * @param jobId JobID
     * @return <tt>true</tt> if the list contains an assignment moving any of the job's tasks
     */
    public boolean containsJob(String jobId) {
        if (jobId == null) {
            return false;
        }
        return moveAssignmentList.stream().anyMatch(assignment -> jobId.equals(assignment.getJobKey().getJobUuid()));
    }

    public List<JobTaskMoveAssignment> getList() {
        return this.moveAssignmentList;
    }

    public boolean add(JobTaskMoveAssignment assignment) {
        bytesUsed += taskSizer.estimateTrueSize(spawn.getTask(assignment.getJobKey()));
        return this.moveAssignmentList.add(assignment);
    }

    public long getBytesUsed() {
        return bytesUsed;
    }
}
