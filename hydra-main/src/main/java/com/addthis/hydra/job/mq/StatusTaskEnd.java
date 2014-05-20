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

import com.addthis.hydra.task.run.TaskExitState;

public class StatusTaskEnd extends AbstractJobMessage {

    private static final long serialVersionUID = -2600124052404240597L;

    private int exitCode;
    private long fileCount;
    private long byteCount;
    private TaskExitState exitState;
    private String rebalanceSource;
    private String rebalanceTarget;
    private boolean wasQueued;

    public StatusTaskEnd(String host, String job, Integer node, int exit, long files, long bytes) {
        super(host, job, node);
        this.exitCode = exit;
        this.fileCount = files;
        this.byteCount = bytes;
    }

    public String getRebalanceSource() {
        return rebalanceSource;
    }

    public void setRebalanceSource(String rebalanceSource) {
        this.rebalanceSource = rebalanceSource;
    }

    public String getRebalanceTarget() {
        return rebalanceTarget;
    }

    public void setRebalanceTarget(String rebalanceTarget) {
        this.rebalanceTarget = rebalanceTarget;
    }

    public StatusTaskEnd setExitState(TaskExitState exitState) {
        this.exitState = exitState;
        return this;
    }

    @Override
    public TYPE getMessageType() {
        return TYPE.STATUS_TASK_END;
    }

    public int getExitCode() {
        return exitCode;
    }

    public long getFileCount() {
        return fileCount;
    }

    public long getByteCount() {
        return byteCount;
    }

    public TaskExitState getExitState() {
        return exitState;
    }

    public boolean wasQueued() {
        return wasQueued;
    }

    public void setWasQueued(boolean wasQueued) {
        this.wasQueued = wasQueued;
    }
}
