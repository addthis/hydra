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

import java.io.Serializable;

import com.addthis.codec.Codec;


public class TaskExitState implements Codec.Codable, Serializable {

    @Codec.Set(codable = true, required = true)
    private boolean hadMoreData;
    @Codec.Set(codable = true)
    private boolean wasStopped;
    @Codec.Set(codable = true)
    private long input;
    @Codec.Set(codable = true)
    private double meanRate;
    @Codec.Set(codable = true)
    private long totalEmitted;

    public void setHadMoreData(boolean hadMoreData) {
        this.hadMoreData = hadMoreData;
    }

    public boolean hadMoreData() {
        return hadMoreData;
    }

    public void setWasStopped(boolean wasStopped) {
        this.wasStopped = wasStopped;
    }

    public boolean getWasStopped() {
        return wasStopped;
    }

    public long getInput() {
        return input;
    }

    public void setInput(long input) {
        this.input = input;
    }

    public double getMeanRate() {
        return meanRate;
    }

    public void setMeanRate(double meanRate) {
        this.meanRate = meanRate;
    }

    public long getTotalEmitted() {
        return totalEmitted;
    }

    public void setTotalEmitted(long totalEmitted) {
        this.totalEmitted = totalEmitted;
    }
}
