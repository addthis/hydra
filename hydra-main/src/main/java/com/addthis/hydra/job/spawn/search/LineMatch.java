package com.addthis.hydra.job.spawn.search;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LineMatch {
    @JsonProperty int lineNum;
    @JsonProperty int startChar;
    @JsonProperty int endChar;

    public LineMatch(int lineNum, int startChar, int endChar) {
        this.lineNum = lineNum;
        this.startChar = startChar;
        this.endChar = endChar;
    }
}
