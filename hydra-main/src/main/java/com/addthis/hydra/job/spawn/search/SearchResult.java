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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sun.istack.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SearchResult {


    @JsonIgnore private final String[] allLines;
    @JsonIgnore private final int bufferLineCount;
    @JsonIgnore private int lastMatchedLine;
    @JsonIgnore private int firstMatchedLine;

    @JsonProperty private final ArrayList<LineMatch> matches;

    public SearchResult(String[] allLines, int bufferLineCount) {
        this.matches = new ArrayList<>();
        this.allLines = allLines;
        this.firstMatchedLine = Integer.MAX_VALUE;
        this.lastMatchedLine = Integer.MIN_VALUE;
        this.bufferLineCount = bufferLineCount;
    }

    /**
     * Creates a list of SearchResults from a list of LineMatches
     * @param lines the entire content of the file where matches are contained
     * @param matches the list of matches in the line
     * @param bufferLineCount how many lines before/after
     * @return the SearchResults which contain every LineMatch provided
     */
    public static List<SearchResult> mergeMatchList(String[] lines, List<LineMatch> matches, int bufferLineCount) {
        Collections.sort(matches);
        Iterator<LineMatch> it = matches.iterator();

        List<SearchResult> results = new ArrayList<>();
        SearchResult result = new SearchResult(lines, bufferLineCount);

        while (it.hasNext()) {
            LineMatch match = it.next();
            if (result.canAddMatchAtLine(match.lineNum)) {
                result.addMatch(match);
            } else {
                results.add(result);
                result = new SearchResult(lines, bufferLineCount);
                result.addMatch(match);
            }
        }

        if (result.hasAnyMatches()) {
            results.add(result);
        }

        return results;
    }

    @JsonProperty("startLine")
    public int getStartLine() {
        return Math.max(firstMatchedLine - bufferLineCount, 0);
    }

    @Nullable
    @JsonProperty("contextLines")
    public String[] getContextLines() {
        if (matches.size() == 0) {
            return null;
        }

        final int start = getStartLine();
        final int end = Math.min(lastMatchedLine + bufferLineCount, allLines.length);

        return Arrays.copyOfRange(allLines, start, end);
    }

    public void addMatch(LineMatch match) {
        if (match.lineNum < firstMatchedLine) {
            firstMatchedLine = match.lineNum;
        }

        if (match.lineNum > lastMatchedLine) {
            lastMatchedLine = match.lineNum;
        }

        matches.add(match);
    }

    public boolean hasAnyMatches() {
        return matches.size() > 0;
    }

    private boolean canAddMatchAtLine(int lineNum) {
        final boolean startsAfter = lineNum > firstMatchedLine - bufferLineCount;
        final boolean endsBefore = lineNum < lastMatchedLine + bufferLineCount;
        return !hasAnyMatches() || (startsAfter && endsBefore);
    }
}
