package com.addthis.hydra.job.spawn.search;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LineMatch implements Comparable<LineMatch> {
    @JsonProperty protected int lineNum;
    @JsonProperty protected int startChar;
    @JsonProperty protected int endChar;

    public LineMatch(int lineNum, int startChar, int endChar) {
        this.lineNum = lineNum;
        this.startChar = startChar;
        this.endChar = endChar;
    }

    @Override
    public int compareTo(LineMatch o) {
        if (lineNum != o.lineNum) {
            return lineNum - o.lineNum;
        }

        return startChar - o.startChar;
    }
}
