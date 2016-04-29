package com.addthis.hydra.job.spawn.search;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * The json result of a search on a single 'file'
 */
public class SearchResult {

    @JsonProperty
    private final String description;

    @JsonProperty
    private final String id;

    @JsonProperty
    private final List<GroupedSearchMatch> results;

    public SearchResult(String id, String description, List<GroupedSearchMatch> results) {
        this.id = id;
        this.description = description;
        this.results = results;
    }
}
