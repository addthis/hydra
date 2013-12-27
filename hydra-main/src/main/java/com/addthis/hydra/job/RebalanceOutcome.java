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
package com.addthis.hydra.job;

import com.addthis.codec.Codec;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * A class describing the outcome of a rebalancing action.
 */
public class RebalanceOutcome implements Codec.Codable {

    @Codec.Set(codable = true)
    private String errMsg;
    @Codec.Set(codable = true)
    private String dirCorrectionMsg;
    @Codec.Set(codable = true)
    private String dirOptimizationMsg;
    @Codec.Set(codable = true)
    private String id;
    private static Logger log = LoggerFactory.getLogger(RebalanceOutcome.class);

    public RebalanceOutcome(String id, String errMsg, String dirCorrectionMsg, String dirOptimizationMsg) {
        this.errMsg = errMsg;
        this.dirCorrectionMsg = dirCorrectionMsg;
        this.dirOptimizationMsg = dirOptimizationMsg;
        this.id = id;
        // Send the outcome to the log also.
        log.warn("[rebalance] outcome: " + toString());
    }

    public boolean failed() {
        return errMsg != null;
    }

    public String toString() {
        if (errMsg != null) {
            return "rebalance failed for " + id + ": \n" + errMsg;
        } else if (dirCorrectionMsg != null) {
            return "rebalance corrected directories for " + id + " : \n" + dirCorrectionMsg;
        } else if (dirOptimizationMsg != null) {
            return "rebalance optimized directories for " + id + " : \n" + dirOptimizationMsg;
        } else {
            return "unexpected RebalanceOutcome input for " + id;
        }
    }
}
