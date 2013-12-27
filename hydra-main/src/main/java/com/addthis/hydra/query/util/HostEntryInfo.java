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
package com.addthis.hydra.query.util;

public class HostEntryInfo {

    private String hostName;
    private int taskId;
    private long lines = 0;
    private long starttime;
    private long endtime;
    private boolean finished = false;
    private boolean ignored = false;

    HostEntryInfo(String hostName, int taskId) {
        this.hostName = hostName;
        this.taskId = taskId;
    }

    public String getHostName() {
        return this.hostName;
    }

    public void start() {
        starttime = System.currentTimeMillis();
    }

    public long getStarttime() {
        return starttime;
    }

    public void setLines(long lines) {
        this.lines = lines;
    }

    public long getLines() {
        return lines;
    }

    public void setFinished() {
        finished = true;
        endtime = System.currentTimeMillis();
    }

    public void setIgnored() {
        ignored = true;
        endtime = System.currentTimeMillis();
    }

    public boolean getFinished() {
        return this.finished;
    }

    public long getEndtime() {
        return this.endtime;
    }

    public long getRuntime() {
        return (endtime == 0) ? System.currentTimeMillis() - starttime : endtime - starttime;
    }

    public boolean isFinished() {
        return finished;
    }

    public int getTaskId() {
        return taskId;
    }
}
