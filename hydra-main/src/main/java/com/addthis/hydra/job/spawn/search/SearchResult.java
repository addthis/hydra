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

import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;

public class SearchResult {


    @JsonIgnore private final String[] allLines;
    @JsonIgnore private final int bufferLineCount;
    @JsonIgnore private int lastMatchedLine;

    @JsonProperty final ArrayList<String> contextLines;
    @JsonProperty final ArrayList<LineMatch> matches;
    @JsonProperty int startLine;

    public SearchResult(String[] lines, int bufferLineCount) {
        this.matches = new ArrayList<>();
        this.contextLines = new ArrayList<>();
        this.allLines = lines;
        this.bufferLineCount = bufferLineCount;
        this.lastMatchedLine = 0;
        this.startLine = 0;
    }


    public void addMatch(int lineNum, int charStart, int charEnd) {
        int endLine = lineNum + bufferLineCount;
        int startLine;

        if (hasAnyMatches()) {
            startLine = lastMatchedLine + bufferLineCount;
        } else {
            startLine = lineNum - bufferLineCount;
            this.startLine = startLine;
        }


        for (int i = startLine; i < endLine; i++) {
            contextLines.add(allLines[i]);
        }

        matches.add(new LineMatch(lineNum, charStart, charEnd));
        lastMatchedLine = lineNum;
    }

    public int lastContextLineNum() {
        return lastMatchedLine + bufferLineCount;
    }

    public boolean hasAnyMatches() {
        return matches.size() > 0;
    }
}
