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
package com.addthis.hydra.job.spawn.search;

import com.addthis.codec.annotations.Bytes;
import com.addthis.codec.annotations.Time;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.TimeUnit;

public class ExpandedConfigCacheSettings {
    public final int maxSizeBytes;
    public final int maxAgeSeconds;

    @JsonCreator
    ExpandedConfigCacheSettings(@Time(TimeUnit.SECONDS) @JsonProperty(value = "maxAgeSeconds", required = true) int maxAgeSeconds,
                                @Bytes @JsonProperty(value = "maxSize", required = true) int maxSizeBytes) {
        this.maxAgeSeconds = maxAgeSeconds;
        this.maxSizeBytes = maxSizeBytes;
    }
}
