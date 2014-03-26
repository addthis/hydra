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
import com.addthis.hydra.job.mq.JobKey;

/**
 */
public class JobTaskDirectoryMatch implements Codec.Codable {

    @Codec.Set(codable = true)
    private final JobKey jobKey;
    @Codec.Set(codable = true)
    private final String hostId;
    @Codec.Set(codable = true)
    private final MatchType type;

    public JobKey getJobKey() {
        return jobKey;
    }

    public String getHostId() {
        return hostId;
    }

    public MatchType getType() {
        return type;
    }


    public JobTaskDirectoryMatch(MatchType type, JobKey jobKey, String hostId) {
        this.type = type;
        this.jobKey = jobKey;
        this.hostId = hostId;
    }

    public String getDirName() {
        return "live";
    }


    public enum MatchType {
        /**
         * The task is on the correct host in the correct form
         */
        MATCH,
        /**
         * Spawn thinks the live should be on this host, but there is no live
         */
        MISMATCH_MISSING_LIVE,
        /**
         * There's a live directory on the host, but Spawn doesn't know about it
         */
        ORPHAN_LIVE,
        /**
         * The task is actively replicating to the target host
         */
        REPLICATE_IN_PROGRESS
    }

    public String getTypeDesc() {
        switch (type) {
            case MATCH:
                return "CORRECT";
            case MISMATCH_MISSING_LIVE:
                return "FAIL: MISSING LIVE";
            case ORPHAN_LIVE:
                return "ORPHAN: LIVE";
            case REPLICATE_IN_PROGRESS:
                return "REPLICATE IN PROGRESS";
            default:
                return "UNKNOWN";
        }
    }

    public String toString() {
        return getDirName() + " on " + hostId + ": " + getTypeDesc();
    }
}
