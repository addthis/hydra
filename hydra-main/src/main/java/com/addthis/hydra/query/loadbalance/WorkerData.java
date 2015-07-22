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

package com.addthis.hydra.query.loadbalance;

import java.util.concurrent.Semaphore;

public class WorkerData {

    public final Semaphore queryLeases;
    public final String hostName;

    public WorkerData(String hostName, int leaseCount) {
        this(hostName, new Semaphore(leaseCount));
    }

    public WorkerData(String hostName, Semaphore queryLeases) {
        this.queryLeases = queryLeases;
        this.hostName = hostName;
    }

    public Semaphore semaphore() { return this.queryLeases; }
    public int queryLeases() { return this.queryLeases.availablePermits(); }
    public String hostName() { return this.hostName; }
}
