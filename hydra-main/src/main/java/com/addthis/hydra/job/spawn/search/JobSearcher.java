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

import com.addthis.hydra.job.JobConfigManager;
import com.addthis.hydra.job.spawn.SpawnState;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
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

    private final SpawnState spawnState;
    private final Pattern pattern;
    private final PipedOutputStream outputStream;
    private final JsonFactory jsonFactory;
    private final ObjectMapper objectMapper;
    private final JobConfigManager jobConfigManager;

    public JobSearcher(SpawnState spawnState, JobConfigManager jobConfigManager, SearchOptions options, PipedOutputStream outputStream) {
        this.spawnState = spawnState;
        this.jobConfigManager = jobConfigManager;
        this.pattern = Pattern.compile(options.pattern);
        this.outputStream = outputStream;
        this.jsonFactory = new JsonFactory();
        this.objectMapper = new ObjectMapper(jsonFactory);
    }

    @Override
    public void run() {

        Iterator<JobIdConfigPair> it = new CacheAwareJobConfigIterator(spawnState, jobConfigManager);
        List<JobIdConfigPair> jobInfoPairs = Lists.newArrayList(it);

        try {
            JsonGenerator generator = jsonFactory.createGenerator(outputStream);
            generator.setCodec(objectMapper);
            try {
                generator.writeStartObject();
                generator.writeNumberField("totalFiles", jobInfoPairs.size());
                generator.writeObjectFieldStart("jobs");

                try {
                    for (JobIdConfigPair jobInfo : jobInfoPairs) {
                        writeExpandedConfigResults(generator, jobInfo);
                    }
                } catch (IOException e) {
                    log.warn("i/o exception writing search result", e);
                }

                generator.writeEndObject();
                generator.writeEndObject();
            } catch(IOException e) {
                log.warn("i/o exception writing search", e);
            } finally {
                generator.close();
            }

        } catch (IOException e) {
            log.warn("i/o exception in search thread", e);
        }
    }

    private void writeExpandedConfigResults(JsonGenerator generator, JobIdConfigPair jobInfo) throws IOException {
        List<SearchResult> jobSearchResults = searchExpandedConfig(jobInfo.config);

        if (jobSearchResults.size() == 0) {
            return;
        }

        generator.writeObjectField(jobInfo.id, jobSearchResults);
    }

    private List<SearchResult> searchExpandedConfig(String expandedConfig) {
        String[] lines = expandedConfig.split("\n");
        List<SearchResult> results = new ArrayList<>();
        SearchResult result = new SearchResult(lines, SEARCH_CONTEXT_BUFFER_LINES);


        for (int lineNum = 0; lineNum < lines.length; lineNum++) {
            if (result.hasAnyMatches() && lineNum > result.lastContextLineNum() + 1) {
                results.add(result);
                result = new SearchResult(lines, SEARCH_CONTEXT_BUFFER_LINES);
            }

            String line = lines[lineNum];
            Matcher m = pattern.matcher(line);
            while (m.find()) {
                result.addMatch(lineNum, m.start(), m.end());
            }
        }

        return results;
    }
}
