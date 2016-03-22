package com.addthis.hydra.job.spawn.search;

public class SearchOptions {
    protected final boolean searchExpanded;
    protected final String pattern;

    public SearchOptions(String pattern) {
        this.pattern = pattern;

        // future features
        this.searchExpanded = true;
    }
}
