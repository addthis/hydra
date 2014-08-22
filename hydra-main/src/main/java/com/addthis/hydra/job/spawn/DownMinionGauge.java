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

import com.yammer.metrics.core.Gauge;

class DownMinionGauge extends Gauge<Integer> {
    private Spawn spawn;

    public DownMinionGauge(Spawn spawn) {
        this.spawn = spawn;
    }

    @Override public Integer value() {
        int total = 0;
        if (spawn.monitored != null) {
            synchronized (spawn.monitored) {
                total = spawn.monitored.size();
            }
        }
        int up;
        if (spawn.minionMembers == null) {
            up = 0;
        } else {
            up = spawn.minionMembers.getMemberSetSize();
        }
        return total - up;
    }
}
