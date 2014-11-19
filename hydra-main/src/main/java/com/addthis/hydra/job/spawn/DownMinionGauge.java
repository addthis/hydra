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
package com.addthis.hydra.job.spawn;

import com.addthis.hydra.job.mq.HostState;

import com.yammer.metrics.core.Gauge;

class DownMinionGauge extends Gauge<Integer> {
    private final HostManager hostManager;

    public DownMinionGauge(HostManager hostManager) {
        this.hostManager = hostManager;
    }

    @Override public Integer value() {
        int down = 0;
        for (HostState host : hostManager.listHostStatus(null)) {
            if ((host != null) && !host.isDead() && !host.isUp()) {
                down++;
            }
        }
        return down;
    }
}
