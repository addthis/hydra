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

    public List<JobTaskMoveAssignment> getList() {
        return this.moveAssignmentList;
    }

    /**
     * Add the given JobTaskMoveAssignment to the moveAssignmentList
     * if the assignment does not move a replica of the same task to the same target host
     * @param assignment JobTaskMoveAssignment to be added to the moveAssignmentList
     * @return <tt>true</tt> if this JobTaskMoveAssignment was added to the moveAssignmentList
     */
    public boolean add(JobTaskMoveAssignment assignment) {
        // Check if moveAssignmentList contains an assignment that moves a replica of the task to the same target host
        boolean isJobKeyPresent = this.moveAssignmentList.stream()
                                                         .anyMatch(moveAssignment ->
                                                                           moveAssignment.getJobKey().equals(assignment.getJobKey()) &&
                                                                           moveAssignment.getTargetUUID().equals(assignment.getTargetUUID()));

        if(!isJobKeyPresent) {
            bytesUsed += taskSizer.estimateTrueSize(spawn.getTask(assignment.getJobKey()));
            this.moveAssignmentList.add(assignment);
            return true;
        }
        return false;
    }

    public long getBytesUsed() {
        return bytesUsed;
    }
}
