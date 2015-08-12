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
package com.addthis.hydra.task.source;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import java.net.HttpURLConnection;
import java.net.URL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import java.nio.file.Files;
import java.nio.file.Path;

import com.addthis.basis.util.LessStrings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.hydra.data.filter.lambda.StringWithValueFilter;
import com.addthis.hydra.data.util.Tokenizer;
import com.addthis.hydra.task.run.TaskRunConfig;
import com.addthis.hydra.task.source.bundleizer.Bundleizer;
import com.addthis.hydra.task.source.bundleizer.BundleizerFactory;
import com.addthis.hydra.task.source.bundleizer.ColumnBundleizer;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.escape.Escaper;
import com.google.common.io.ByteStreams;
import com.google.common.net.UrlEscapers;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This data source <span class="hydra-summary">submits a hydra query for data</span>.
 *
 * <p>The user specifies the job identifier, query path, and query operations
 * of the query to execute. Queries are submitted to the query master to coordinate
 * scheduling. Query operations are not performed on the query master. In other words
 * the {@link #ops} parameter is identical to the "remote ops" of a query submitted to
 * the query system. You may optionally specify which specific tasks of the job should
 * be queried. If you do not specify any tasks then the tasks of the job you are querying
 * will be evenly distributed across the tasks of the job using this input source.
 *
 * <p>The query input source currently forbids partial results from being processed.
 * If the query is not finished when the job is stopped then the job will error.
 * This is a stop-gap measure until we determine a comprehensive solution for partial
 * query results.
 *
 * <p>The query input source specifies a mark file. If the query runs to completion
 * then the mark file will be created. The next time the job runs it will not submit
 * another query.
 *
 * @user-reference
 */
public class DataSourceQuery extends TaskDataSource {

    static final int LOG_TRUNCATE_CHARS = 500;

    private static final Joiner AMPERSAND_JOINER = Joiner.on('&');

    private static final Joiner COMMA_JOINER = Joiner.on(',');

    private static final Logger log = LoggerFactory.getLogger(DataSourceQuery.class);

    /**
     * Request that the query system format output
     * in the version of csv output that corrects
     * escapes characters.
     */
    private static final String QUERY_FORMAT = "csv2";

    /**
     * Separator in csv format.
     */
    private static final String TOKEN_SEPARATOR = ",";

    /**
     * Group character in csv format.
     */
    private static final String[] TOKEN_GROUPS = {"\""};

    /**
     * URL to submit a query. Typical url is
     * "http://[hostname]:2222/query/call"
     */
    private static final String QUERY_URL = "http://%s:%d%s";

    /**
     * Query master host name. This field is required.
     */
    @JsonProperty(required = true) private String mqmaster;

    /**
     * Query master port number. Default value is 2222.
     */
    @JsonProperty(required = true) private int port;

    /**
     * Query master url. Default is "/query/call"
     */
    @JsonProperty(required = true) private String urlPath;

    /**
     * The tree job that is to be queried. Specify no jobId
     * for a job to query itself. This field is optional.
     */
    @JsonProperty private String jobId;

    /**
     * Optionally specify the directory
     * of the tree for the given job. Default
     * is "data".
     */
    @JsonProperty(required = true) private String treeDir;

    /**
     * Path for the query. This field is required.
     */
    @JsonProperty(required = true) private StringWithValueFilter path;

    /**
     * Operations for the query. This field is optional.
     */
    @JsonProperty private String ops;

    /**
     * Which tasks to query. This field is optional.
     */
    @JsonProperty private Integer[] tasks;

    /** Number of tasks in the query job. This field is required. */
    @JsonProperty(required = true) private int taskTotal;

    /**
     * Specify the field names for the columns
     * produced by the query. This field is required.
     */
    @JsonProperty(required = true) private String[] fields;

    /**
     * Path to the mark file. Default is "markfile".
     */
    @JsonProperty private Path markFile;

    /** Ignore the mark file */
    @JsonProperty private boolean ignoreMarkFile;

    @JsonProperty private TaskRunConfig config;

    private final AtomicBoolean queryCompleted = new AtomicBoolean(false);

    private final AtomicReference<Exception> firstError = new AtomicReference<>(null);

    private Bundleizer bundleizer;
    private Bundle nextBundle;
    private InputStream underlyingInputStream;

    @Override
    public void init(boolean concurrent) {
        if (testMarkFile()) {
            return;
        }
        HttpURLConnection conn = null;
        tasks = buildTasks(tasks);
        try {
            URL url = new URL(String.format(QUERY_URL, mqmaster, port, urlPath));
            conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            Map<String, String> parameters = new HashMap<>();
            parameters.put("sender", config.jobIdWithNode());
            parameters.put("job", jobId);
            parameters.put("path", path.get());
            parameters.put("dir", treeDir);
            // the query master should not perform any ops on these queries
            parameters.put("rops", ops);
            parameters.put("tasks", COMMA_JOINER.join(tasks));
            parameters.put("format", QUERY_FORMAT);
            writePostBody(conn, parameters);
            underlyingInputStream = conn.getInputStream();
            Tokenizer tokenizer = new Tokenizer(TOKEN_SEPARATOR, TOKEN_GROUPS, false);
            BundleizerFactory format = new ColumnBundleizer(fields, tokenizer, null);
            bundleizer = format.createBundleizer(underlyingInputStream, new ListBundle());
        } catch (IOException outer) {
            if ((conn != null) && (conn.getErrorStream() != null)) {
                try {
                    log.error("URL connection was unsuccessful. Response is {}",
                              new String(ByteStreams.toByteArray(conn.getErrorStream())));
                } catch (IOException inner) {
                    log.error("During connection error failure to read error stream: ", inner);
                }
            }
            firstError.compareAndSet(null, outer);
            throw Throwables.propagate(outer);
        }
    }

    /**
     * Deletes the existing mark file if ignoreMarkFile
     * field is set. Tests if the mark file exists.
     *
     * @return true if the mark file exists
     */
    private boolean testMarkFile() {
        if (markFile != null) {
            if (ignoreMarkFile) {
                try {
                    boolean deleted = Files.deleteIfExists(markFile);
                    if (deleted) {
                        log.warn("Deleted mark file : {}", markFile);
                    }
                } catch (IOException ex) {
                    firstError.compareAndSet(null, ex);
                    throw Throwables.propagate(ex);
                }
            }
            if (Files.exists(markFile)) {
                queryCompleted.set(true);
                return true;
            }
        }
        return false;
    }

    /**
     * If a list of tasks is provided as input then perform validation
     * on the tasks numbers. If a list of tasks is not provided then
     * generate a list of tasks.
     */
    private Integer[] buildTasks(Integer[] tasks) {
        if (tasks != null) {
            List<Integer> outBounds = new ArrayList<>();
            for (Integer task : tasks) {
                if ((task < 0) || (task >= taskTotal)) {
                    outBounds.add(task);
                }
            }
            if (outBounds.size() > 0) {
                throw new IllegalArgumentException("The following provided task numbers " +
                                                   "are out of bounds: " + outBounds);
            }
            return tasks;
        } else {
            return config.calcShardList(taskTotal);
        }
    }

    private static void writePostBody(HttpURLConnection conn, Map<String, String> parameters) throws IOException {
        try (OutputStream os = conn.getOutputStream()) {
            Escaper escaper = UrlEscapers.urlFormParameterEscaper();
            // Select non-null (key, value) pairs and join them
            List<String> kvpairs = parameters.entrySet().stream()
                                             .filter((e) -> (e.getValue() != null))
                                             .map((e) -> (escaper.escape(e.getKey()) + "=" +
                                                          escaper.escape(e.getValue()))).collect(Collectors.toList());
            String content = AMPERSAND_JOINER.join(kvpairs);
            log.info("First {} characters of POST body are {}", LOG_TRUNCATE_CHARS,
                     LessStrings.trunc(content, LOG_TRUNCATE_CHARS));
            os.write(content.getBytes());
            os.flush();
        }
    }

    @Override public Bundle next() throws DataChannelError {
        if (nextBundle != null) {
            Bundle result = nextBundle;
            nextBundle = null;
            return result;
        } else {
            try {
                if (bundleizer != null) {
                    Bundle next = bundleizer.next();
                    if (next == null) {
                        queryCompleted.set(true);
                        if (markFile != null) {
                            Files.createFile(markFile);
                        }
                    }
                    return next;
                } else {
                    return null;
                }
            } catch (Exception ex) {
                firstError.compareAndSet(null, ex);
                throw Throwables.propagate(ex);
            }
        }
    }

    @Override public Bundle peek() throws DataChannelError {
        if (nextBundle == null) {
            try {
                if (bundleizer != null) {
                    nextBundle = bundleizer.next();
                }
            } catch (Exception ex) {
                firstError.compareAndSet(null, ex);
                throw Throwables.propagate(ex);
            }
        }
        return nextBundle;
    }

    @Override public void close() {
        try {
            if (underlyingInputStream != null) {
                underlyingInputStream.close();
            }
            //noinspection ThrowableResultOfMethodCallIgnored
            if (!queryCompleted.get() && (firstError.get() == null)) {
                throw new UnsupportedOperationException("Query did not complete before job completed. " +
                                                        "Partial query results are currently not " +
                                                        "allowed by the query input source");
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public @Nonnull ImmutableList<Path> writableRootPaths() {
        return ImmutableList.of(markFile);
    }

}
