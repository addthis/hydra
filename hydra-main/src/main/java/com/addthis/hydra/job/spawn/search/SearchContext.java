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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

public class SearchContext {
    @JsonProperty
    protected final int startLine;
    @JsonProperty
    protected final String[] lines;

    public SearchContext(int matchedLine, String[] allLines, int buffer) {
        this.startLine = Math.max(0, matchedLine - buffer);
        int endLine = Math.min(allLines.length, matchedLine + buffer);
        this.lines = Arrays.copyOfRange(allLines, startLine, endLine);
    }
}
