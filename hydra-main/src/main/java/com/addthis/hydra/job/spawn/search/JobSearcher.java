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

import com.addthis.codec.jackson.Jackson;
import com.addthis.hydra.job.Job;
import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.entity.JobMacro;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


/**
 * Searches job configurations based on the search options passed to the constructor.
 *
 * @return
 * @throws Exception
 */
public class JobSearcher implements Runnable {
    // The number of lines to put above and below a matching line to provide some context
    private final static int SEARCH_CONTEXT_BUFFER_LINES = 3;
    private static final Logger log = LoggerFactory.getLogger(JobSearcher.class);

    private final Pattern pattern;
    private final Map<String, JobMacro> macros;
    private final Map<String, Job> jobs;
    private final JobConfigManager jobConfigManager;
    private final JsonGenerator generator;

    public JobSearcher(Map<String, Job> jobs, Map<String, JobMacro> macros, JobConfigManager jobConfigManager, SearchOptions options, PipedOutputStream outputStream) throws IOException {
        this.jobs = jobs;
        this.macros = macros;
        this.jobConfigManager = jobConfigManager;
        this.pattern = Pattern.compile(options.pattern);
        this.generator = Jackson.defaultMapper().getFactory().createGenerator(outputStream);
    }

    @Override
    public void run() {
        try {
            generator.writeStartObject();

            JobMacroGraph dependencyGraph = new JobMacroGraph(macros);
            Map<String, Set<TextLocation>> macroSearches = searchMacros(macros, dependencyGraph);

            /*
             {
               jobs: [
                 {id, description, matches: []}
               ]
               macros: [
                 {id, description, matches: []}
               ]
             }
             */
            generator.writeObjectField("macros", getMacroSearchResults(macroSearches, macros));

            generator.writeArrayFieldStart("jobs");
            for (Job job : jobs.values()) {
                SearchResult jobSearchResult = getJobSearchResult(job, macroSearches);
                if (jobSearchResult != null) {
                    generator.writeObject(jobSearchResult);
                }
            }
            generator.writeEndArray();

            generator.writeEndObject();
        } catch (Exception e) {
            log.error("JobSearcher failed:", e);
        } finally {
            try {
                generator.close();
            } catch (IOException e) {
                log.error("JobSearcher generator failed to close", e);
            }
        }

    }

    @Nullable
    private SearchResult getJobSearchResult(Job job, Map<String, Set<TextLocation>> macroSearches) {
        String config = jobConfigManager.getConfig(job.getId());
        SearchableItem si = new SearchableItem(config, job.getId());
        MacroIncludeLocations macroIncludeLocations = new MacroIncludeLocations(config);

        Set<TextLocation> searchLocs = si.search(pattern);

        // For each macro dependency of the job, see if that macro (or any of its dependencies) contains a search result
        searchLocs.addAll(getDependencySearchMatches(macroIncludeLocations, macroSearches));

        List<GroupedSearchMatch> groups = GroupedSearchMatch.mergeMatchList(config.split("\n"), searchLocs);

        if (groups.size() > 0) {
            return new SearchResult(job.getId(), job.getDescription(), groups);
        } else {
            return null;
        }
    }

    @Nullable
    private SearchResult getMacroSearchResult(String macroName, Set<TextLocation> macroSearch) {
        JobMacro macro = macros.get(macroName);

        if (macro == null) {
            throw new NullPointerException();
        }

        String[] macroLines = macro.getMacro().split("\n");
        List<GroupedSearchMatch> groupedSearchMatches = GroupedSearchMatch.mergeMatchList(macroLines, macroSearch);

        if (groupedSearchMatches.size() > 0) {
            return new SearchResult(macroName, "", groupedSearchMatches);
        } else {
            return null;
        }
    }


    private List<SearchResult> getMacroSearchResults(Map<String, Set<TextLocation>> macroSearches, Map<String, JobMacro> macros) {
        List<SearchResult> results = new ArrayList<>();
        for (String macroName : macroSearches.keySet()) {
            SearchResult result = getMacroSearchResult(macroName, macroSearches.get(macroName));
            if (result != null) {
                results.add(result);
            }
        }

        return results;
    }

    private Map<String, Set<TextLocation>> searchMacros(Map<String, JobMacro> macros, JobMacroGraph dependencyGraph) {
        Map<String, Set<TextLocation>> results = new HashMap<>();

        // Search the text of the macros themselves for any results
        for (String macroName : macros.keySet()) {
            JobMacro macro = macros.get(macroName);
            SearchableItem macroSearch = new SearchableItem(macro.getMacro(), macroName);
            results.put(macroName, macroSearch.search(pattern));
        }

        // Macros can include other macros, so we need to add dependent macros (which contain search results) to the
        // parent macro's search results.
        for (String macroName : results.keySet()) {
            Set<TextLocation> macroSearchResults = results.get(macroName);
            MacroIncludeLocations macroIncludeLocations = dependencyGraph.getIncludeLocations(macroName);

            // See if any of the (recursive) dependencies of this macro had search results. If they do, we add a new
            // LineMatch to this macro's search results, which indicates where the macro w/ a search result was included
            macroSearchResults.addAll(getDependencySearchMatches(macroIncludeLocations, results));
        }

        return results;
    }

    private ImmutableSet<TextLocation> getDependencySearchMatches(MacroIncludeLocations macroIncludeLocations,
                                                                  Map<String, Set<TextLocation>> macroSearchResults) {
        ImmutableSet.Builder<TextLocation> builder = ImmutableSet.builder();
        for (String depMacroName : macroIncludeLocations.dependencies()) {
            Set<TextLocation> depMacroResults = macroSearchResults.getOrDefault(depMacroName, ImmutableSet.of());
            if (!depMacroResults.isEmpty()) {
                builder.addAll(macroIncludeLocations.locationsFor(depMacroName));
            }
        }

        return builder.build();
    }
}
