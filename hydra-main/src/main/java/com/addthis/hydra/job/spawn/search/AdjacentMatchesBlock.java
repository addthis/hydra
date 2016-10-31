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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sun.istack.Nullable;

/**
 * Represents a single group of contiguous lines from one section of one 'file', which in itself may contain many
 * matches.
 */
public class AdjacentMatchesBlock {
    private static final int BUFFER_LINE_COUNT = 3;

    @JsonIgnore
    private final String[] allLines;
    @JsonProperty
    private final ArrayList<TextLocation> matches;
    @JsonIgnore
    private int lastMatchedLine;
    @JsonIgnore
    private int firstMatchedLine;

    private AdjacentMatchesBlock(String[] allLines) {
        this.matches = new ArrayList<>();
        this.allLines = allLines;
        this.firstMatchedLine = Integer.MAX_VALUE;
        this.lastMatchedLine = Integer.MIN_VALUE;
    }

    /**
     * Creates a list of {@link AdjacentMatchesBlock} from a list of {@link TextLocation}
     *
     * @param lines   the entire content of the file where matches are contained
     * @param matches the list of matches in the line
     * @return the {@link AdjacentMatchesBlock} which contain every {@link TextLocation} provided
     */
    public static List<AdjacentMatchesBlock> mergeMatchList(String[] lines, Collection<TextLocation> matches) {
        List<TextLocation> sortedMatches = new ArrayList<>(matches);
        Collections.sort(sortedMatches);
        Iterator<TextLocation> it = sortedMatches.iterator();

        List<AdjacentMatchesBlock> results = new ArrayList<>();
        AdjacentMatchesBlock result = new AdjacentMatchesBlock(lines);

        TextLocation match = null;
        TextLocation prev = null;
        while (it.hasNext()) {
            prev = match;
            match = it.next();
            // ignore current match if it's duplicate of the previous one. Note that the matches are sorted by
            // starting poisition, so for any duplicate, previous match sholud fully contain the current one.
            if (prev != null && prev.fullyContains(match)) {
                continue;
            }
            if (result.canAddMatchAtLine(match.lineNum)) {
                result.addMatch(match);
            } else {
                results.add(result);
                result = new AdjacentMatchesBlock(lines);
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
        return Math.max(firstMatchedLine - BUFFER_LINE_COUNT, 0);
    }

    @Nullable
    @JsonProperty("contextLines")
    public String[] getContextLines() {
        if (matches.size() == 0) {
            return null;
        }

        final int start = getStartLine();
        final int end = Math.min(lastMatchedLine + BUFFER_LINE_COUNT, allLines.length);

        return Arrays.copyOfRange(allLines, start, end);
    }

    private void addMatch(TextLocation match) {
        firstMatchedLine = Math.min(match.lineNum, firstMatchedLine);
        lastMatchedLine = Math.max(match.lineNum, lastMatchedLine);
        matches.add(match);
    }

    public boolean hasAnyMatches() {
        return matches.size() > 0;
    }

    private boolean canAddMatchAtLine(int lineNum) {
        final boolean startsAfter = lineNum > firstMatchedLine - BUFFER_LINE_COUNT;
        final boolean endsBefore = lineNum < lastMatchedLine + BUFFER_LINE_COUNT;
        return !hasAnyMatches() || (startsAfter && endsBefore);
    }
}
