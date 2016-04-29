package com.addthis.hydra.job.spawn.search;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SearchableItem {
    private final String[] lines;
    private final String id;

    public SearchableItem(String text, String id) {
        this.lines = text.split("\n");
        this.id = id;
    }

    public Set<TextLocation> search(Pattern pattern) {
        Set<TextLocation> matchSet = new HashSet<>();

        for (int lineNum = 0; lineNum < lines.length; lineNum++) {
            String line = lines[lineNum];
            Matcher m = pattern.matcher(line);
            while (m.find()) {
                matchSet.add(new TextLocation(lineNum, m.start(), m.end()));
            }
        }

        return matchSet;
    }
}
