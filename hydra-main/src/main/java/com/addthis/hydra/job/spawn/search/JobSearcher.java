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
import com.addthis.hydra.job.JobParameter;
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
import java.util.function.Predicate;
import java.util.regex.Pattern;


/**
 * Searches job configurations based on the search options passed to the constructor.
 *
 * @return
 * @throws Exception
 */
public class JobSearcher implements Runnable {
    // The number of lines to put above and below a matching line to provide some context
    private static final Logger log = LoggerFactory.getLogger(JobSearcher.class);

    private final Pattern pattern;
    private final Map<String, JobMacro> macros;
    private final Map<String, Job> jobs;
    private final JobConfigManager jobConfigManager;
    private final JsonGenerator generator;
    private final Map<String, List<String>> aliases;

    public JobSearcher(Map<String, Job> jobs,
                       Map<String, JobMacro> macros,
                       Map<String, List<String>> aliases,
                       JobConfigManager jobConfigManager,
                       SearchOptions options,
                       PipedOutputStream outputStream) throws IOException {
        this.jobs = jobs;
        this.macros = macros;
        this.aliases = aliases;
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
            generator.writeObjectField("macros", getMacroSearchResults(macroSearches));

            generator.writeArrayFieldStart("jobs");
            for (Job job : jobs.values()) {
                SearchResult jobSearchResult = searchJob(job, dependencyGraph, macroSearches);
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
    private SearchResult searchJob(Job job, JobMacroGraph dependencyGraph, Map<String, Set<TextLocation>> macroSearches) {
        String config = jobConfigManager.getConfig(job.getId());
        IncludeLocations macroIncludeLocations = IncludeLocations.forMacros(config);

        Predicate<String> predicate = pattern.asPredicate();
        Set<TextLocation> searchLocs = LineSearch.search(config, pattern);

        // For each macro dependency of the job, see if that macro (or any of its dependencies) contains a search result
        searchLocs.addAll(getDependencySearchMatches(macroIncludeLocations, dependencyGraph, macroSearches));

        // For each alias in the job, see if any of the job IDs which that alias point to contain a search result
        // Macros and aliases have identical syntax -- the same locations map can be used for either one
        searchLocs.addAll(getMatchedAliasLocations(macroIncludeLocations));

        // For each parameter of the job, see if that parameter contains a search result
        IncludeLocations paramIncludeLocations = IncludeLocations.forJobParams(config);
        for (JobParameter param : job.getParameters()) {
            String paramValue = param.getValueOrDefault();
            if (predicate.test(paramValue)) {
                searchLocs.addAll(paramIncludeLocations.locationsFor(param.getName()));
            }

            // Sadly, these parameters might ALSO contain macros, aliases, etc. So test that, too
            IncludeLocations nestedIncludeLocations = IncludeLocations.forMacros(paramValue);
            searchLocs.addAll(getMatchedAliasLocations(nestedIncludeLocations));
            searchLocs.addAll(getDependencySearchMatches(nestedIncludeLocations, dependencyGraph, macroSearches));
        }

        // Merge the matches together into groups which can be easily displayed on the client
        List<AdjacentMatchesBlock> groups = AdjacentMatchesBlock.mergeMatchList(config.split("\n"), searchLocs);

        if (groups.size() > 0) {
            return new SearchResult(job.getId(), job.getDescription(), groups);
        } else {
            return null;
        }
    }

    private Collection<TextLocation> getMatchedAliasLocations(IncludeLocations macroIncludeLocations) {
        Predicate<String> predicate = pattern.asPredicate();
        ImmutableSet.Builder<TextLocation> results = ImmutableSet.builder();

        for (String alias : aliases.keySet()) {
            for (String jobID : aliases.get(alias)) {
                if (predicate.test(jobID)) {
                    results.addAll(macroIncludeLocations.locationsFor(alias));
                }
            }
        }

        return results.build();
    }


    @Nullable
    private SearchResult getMacroSearchResult(String macroName, Set<TextLocation> macroSearch) {
        JobMacro macro = macros.get(macroName);

        if (macro == null) {
            throw new NullPointerException();
        }

        String[] macroLines = macro.getMacro().split("\n");
        List<AdjacentMatchesBlock> adjacentMatchesBlocks = AdjacentMatchesBlock.mergeMatchList(macroLines, macroSearch);

        if (adjacentMatchesBlocks.size() > 0) {
            return new SearchResult(macroName, "", adjacentMatchesBlocks);
        } else {
            return null;
        }
    }


    private List<SearchResult> getMacroSearchResults(Map<String, Set<TextLocation>> macroSearches) {
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
            results.put(macroName, LineSearch.search(macro.getMacro(), pattern));
        }

        // Macros can include other macros, so we need to add dependent macros (which contain search results) to the
        // parent macro's search results.
        for (String macroName : results.keySet()) {
            Set<TextLocation> macroSearchResults = results.get(macroName);
            IncludeLocations macroIncludeLocations = dependencyGraph.getIncludeLocations(macroName);

            // See if any of the (recursive) dependencies of this macro had search results. If they do, we add a new
            // LineMatch to this macro's search results, which indicates where the macro w/ a search result was included
            macroSearchResults.addAll(getDependencySearchMatches(macroIncludeLocations, dependencyGraph, results));
        }

        return results;
    }


    /**
     * Returns a set of TextLocations which represent that an included dependency (or one of the included dependency's
     * dependencies, etc.) contains a search match
     *
     * @param macroIncludeLocations
     * @param dependencyGraph
     * @param macroSearchResults
     * @return
     */
    private ImmutableSet<TextLocation> getDependencySearchMatches(IncludeLocations macroIncludeLocations,
                                                                  JobMacroGraph dependencyGraph,
                                                                  Map<String, Set<TextLocation>> macroSearchResults) {

        ImmutableSet.Builder<TextLocation> builder = ImmutableSet.builder();
        for (String depMacroName : macroIncludeLocations.dependencies()) {

            // For each dependency that depMacroName brings in, see if any of THEM have search results.  If they do, we
            // want to link back to the include to depMacroName in the search result.
            for (String deeperDepName : dependencyGraph.getDependencies(depMacroName)) {
                Set<TextLocation> depMacroResults = macroSearchResults.getOrDefault(deeperDepName, ImmutableSet.of());
                if (!depMacroResults.isEmpty()) {
                    builder.addAll(macroIncludeLocations.locationsFor(depMacroName));
                    break;
                }
            }
        }

        return builder.build();
    }
}
