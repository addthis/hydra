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

import com.addthis.hydra.job.spawn.Spawn;
import com.addthis.maljson.JSONException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
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

    private final Spawn spawn;
    private final SearchOptions options;
    private final Pattern pattern;
    private final PipedOutputStream outputStream;

    public JobSearcher(Spawn spawn, SearchOptions options, PipedOutputStream outputStream) {
        this.spawn = spawn;
        this.options = options;
        this.pattern = Pattern.compile(options.pattern);
        this.outputStream = outputStream;
    }

    @Override
    public void run() {

        Iterator<String> it = spawn.getSpawnState().jobIdIterator();

        List<String> jobIdsNotInCache = new ArrayList<>();

        try {
            outputStream.write("{\"search\": [".getBytes());
            // Attempt to search through cached configs first
            while (it.hasNext()) {
                String jobId = it.next();
                String expandedConfig = spawn.getCachedExpandedJob(jobId);

                if (expandedConfig == null) {
                    jobIdsNotInCache.add(jobId);
                    continue;
                }

                searchExpandedConfig(jobId, expandedConfig);
            }

            for (String jobId : jobIdsNotInCache) {
                try {
                    searchExpandedConfig(jobId, spawn.getExpandedConfig(jobId));
                } catch (ExecutionException e) {
                    log.warn("failed to get expanded config", e);
                }
            }
            outputStream.write("]}".getBytes());
            outputStream.flush();
        } catch (IOException e) {
            log.warn("i/o exception in search thread", e);
        } finally {
            try {
                outputStream.close();
            } catch (IOException e) {
                log.warn("failed to close jobsearcher", e);
            }
        }

    }

    private void searchExpandedConfig(String jobId, String expandedConfig) throws IOException {
        String[] lines = expandedConfig.split("\n");

        for (int lineNum = 0; lineNum < lines.length; lineNum++) {
            String line = lines[lineNum];
            Matcher m = pattern.matcher(line);
            while (m.find()) {
                SearchContext context = new SearchContext(lineNum, lines, SEARCH_CONTEXT_BUFFER_LINES);
                SearchResult sr = new SearchResult(jobId, lineNum, m.start(), m.end(), context);
                try {
                    outputStream.write(sr.serialize());
                    outputStream.write("\n".getBytes());
                    outputStream.flush();
                } catch (JSONException e) {
                    String out = "{\"error\": \"" + StringEscapeUtils.escapeEcmaScript(e.getMessage()) + "\"}\n";
                    outputStream.write(out.getBytes());
                    outputStream.flush();
                    log.warn("failed to serialize search result", e);
                }
            }
        }
    }
}
