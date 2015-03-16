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

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import java.nio.file.Paths;

import com.addthis.basis.util.LessFiles;
import com.addthis.basis.util.Parameter;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.util.AutoField;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.data.query.FramedDataChannelReader;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.source.QueryEngineSourceSingle;
import com.addthis.hydra.data.query.source.QueryHandle;
import com.addthis.hydra.data.tree.ReadTree;
import com.addthis.hydra.task.run.TaskRunConfig;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;
import com.addthis.meshy.service.stream.StreamSource;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceQuery extends AbstractStreamDataSource implements DataChannelOutput {

    public static final int FRAME_READER_POLL = Parameter.intValue("meshSourceAggregator.frameReader.poll", 0);

    private final ExecutorService workerThreadPool = Executors.newFixedThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("dataSourceQuery-%d").build());

    private static final Logger log = LoggerFactory.getLogger(DataSourceQuery.class);

    private final BundleFormat formatIn = new ListBundleFormat();

    private final BundleFormat formatOut = new ListBundleFormat();

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
    @JsonProperty(required = true) private String path;

    /**
     * Operations for the query. This field is optional.
     */
    @JsonProperty private String ops;

    /**
     * Which tasks to query. This field is optional.
     */
    @JsonProperty private int[] tasks;

    /** Number of tasks in the query job. This field is required. */
    @JsonProperty(required = true) private int taskTotal;

    /**
     * Optionally specify the field names for the columns
     * produced by the query. Otherwise the fields will
     * be named "0", "1", "2", etc. Default is null.
     */
    @JsonProperty private AutoField[] fields;

    @JsonProperty private TaskRunConfig config;

    /**
     * Hostname of the query system mesh network. Default is 'localhost'
     */
    @JsonProperty
    private String queryHost;

    /**
     * Port number of the query system mesh network. Default is
     * value of system property 'qmaster.mesh.port'.
     */
    @JsonProperty private int queryPort;

    private final AtomicReference<Throwable> firstError = new AtomicReference<>();

    private QueryEngineSourceSingle queryEngine;

    private QueryHandle queryHandle;

    CompletableFuture<Void> workerFuture;

    private File tempDir;

    private MeshyClient meshClient;

    private boolean localInputSource;

    private static String getFileMatchString(String job, String treeDir, String task) {
        return "*/" + job + "/" + task + "/gold/" + treeDir + "/query";
    }

    private static Joiner COMMA_JOINER = Joiner.on(',');

    private static String printTasks(int[] tasks) {
        String result = "{";
        result += COMMA_JOINER.join(Ints.asList(tasks));
        result += "}";
        return result;
    }

    @Override public void init() {
        super.init();
        localInputSource = (jobId == null);
        try {
            if (localInputSource) {
                File dir = Paths.get(treeDir).toFile();
                tempDir = LessFiles.createTempDir();
                queryEngine = new QueryEngineSourceSingle(new ReadTree(dir));
                Query query = new Query(config.jobId, new String[]{path}, null);
                QueryOpProcessor proc = new QueryOpProcessor.Builder(this, new String[]{ops})
                        .tempDir(tempDir).build();
                queryHandle = queryEngine.query(query, proc);
                initialized.countDown();
                localInitialized = true;
            } else {
                if (tasks != null) {
                    List<Integer> outBounds = Ints.asList(tasks).stream().filter(
                            (x) -> ((x < 0) || (x >= taskTotal))).collect(Collectors.toList());
                    if (outBounds.size() > 0) {
                        throw new IllegalArgumentException("The following provided task numbers " +
                                                           "are out of bounds: " + outBounds);
                    }
                } else {
                    List<Integer> taskList = new ArrayList<>();
                    int current = config.node;
                    while (current < taskTotal) {
                        taskList.add(current);
                        current += config.nodeCount;
                    }
                    tasks = Ints.toArray(taskList);
                }
                Query query = new Query(config.jobId, new String[]{path}, new String[]{ops});
                Map queryOptions = new HashMap<>();
                queryOptions.put("query", CodecJSON.encodeString(query));
                meshClient = new MeshyClient(queryHost, queryPort);
                String meshFilePath = getFileMatchString(jobId, treeDir, printTasks(tasks));
                FileSource source = new FileSource(meshClient);
                source.requestRemoteFiles(meshFilePath);
                source.waitComplete();
                // collapse replicas of a file reference
                Collection<FileReference> fileReferences = source.getFileMap().values();
                log.info("Queries will run against {}", fileReferences.toString());
                Runnable sourceWorker = new SourceWorker(fileReferences.iterator(), queryOptions);
                workerFuture = CompletableFuture.runAsync(sourceWorker, workerThreadPool).whenComplete(
                        (ignored, error) -> {
                            if (error != null) {
                                shuttingDown.set(true);
                                closeFuture.completeExceptionally(error);
                            }
                        });
                workerFuture.thenRunAsync(this::close);
            }
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    private<T> void cleanupResource(T target, Consumer<T> operation, String errorMessage) {
        try {
            if (target != null) {
                operation.accept(target);
            }
        } catch (Exception ex) {
            firstError.compareAndSet(null, ex);
            log.error(errorMessage, ex);
        }
    }

    @Override public void close() {
        if (shuttingDown.compareAndSet(false, true)) {
            cleanupResource(queryHandle, (x) -> x.cancel("closing query input source"),
                            "Error attempting to cancel local query");
            cleanupResource(queryEngine, (x) -> x.close(), "Error attempting to close local query engine");
            cleanupResource(tempDir, (x) -> LessFiles.deleteDir(x), "Error attempting to delete local temporary directory");
            cleanupResource(meshClient, (x) -> x.close(), "Error attempting to close mesh link");
            cleanupResource(workerThreadPool, (x) -> x.shutdown(), "Error attempting to shutdown worker thread pool");
            Throwable error = firstError.get();
            if (error != null) {
                closeFuture.completeExceptionally(error);
                throw Throwables.propagate(error);
            } else {
                closeFuture.complete(null);
            }
        } else {
            closeFuture.join();
        }
    }

    private static String[] fieldNames(Bundle bundle) {
        return FluentIterable.from(bundle.getFormat()).transform((field) -> field.getName()).toArray(String.class);
    }

    @Override public void send(Bundle input) {
        try {
            Bundle output;
            if (fields == null) {
                output = input;
            } else {
                if (fields.length != input.getFormat().getFieldCount()) {
                    throw new IllegalStateException("Expecting " + fields.length + " fields " +
                                                    Arrays.toString(fields) + " and result from query has " +
                                                    input.getFormat().getFieldCount() + " fields " +
                                                    Arrays.toString(fieldNames(input)));
                }
                output = formatOut.createBundle();
                Iterator<BundleField> fieldsIn = input.getFormat().iterator();
                for (int i = 0; i < fields.length; i++) {
                    AutoField fieldOut = fields[i];
                    fieldOut.setValue(output, input.getValue(fieldsIn.next()));
                }
            }
            queue.put(output);
        } catch (Exception ex) {
            firstError.compareAndSet(null, ex);
            close();
        }
    }

    @Override public void send(List<Bundle> list) {
        list.forEach(this::send);
    }

    @Override public void sendComplete() {
        close();
    }

    @Override public void sourceError(Throwable throwable) {
        firstError.compareAndSet(null, throwable);
        close();
    }

    @Override public Bundle createBundle() {
        return formatIn.createBundle();
    }

    private class SourceWorker implements Runnable {

        private final Iterator<FileReference> fileReferences;

        private final Map<String, String> queryOptions;

        private FramedDataChannelReader dataChannel;

        SourceWorker(Iterator<FileReference> fileReferences, Map<String,String> queryOptions) {
            this.fileReferences = fileReferences;
            this.queryOptions = queryOptions;
        }

        @Override public void run() {
            try {
                while (!shuttingDown.get()) {
                    if (readChannel()) return;
                }
            } catch (Exception ex) {
                log.warn("Exception while running data source query worker thread.", ex);
                Throwables.propagate(ex);
            } finally {
                if (dataChannel != null) {
                    try {
                        dataChannel.close();
                    } catch (IOException ex) {
                        log.info("IO error trying to close data channel", ex);
                    }
                }
            }
        }

        private boolean readChannel() throws Exception {
            if (dataChannel == null) {
                if (fileReferences.hasNext()) {
                    FileReference reference = fileReferences.next();
                    StreamSource streamSource = meshClient.getFileSource(reference.getHostUUID(),
                                                                         reference.name,
                                                                         queryOptions);
                    dataChannel = new FramedDataChannelReader(streamSource, FRAME_READER_POLL);
                } else {
                    close();
                    return true;
                }
            }
            Bundle next = dataChannel.read();
            if (next != null) {
                while (!queue.offer(next, 1, TimeUnit.SECONDS)) {
                    if (shuttingDown.get()) {
                        return true;
                    }
                }
                if (!localInitialized) {
                    initialized.countDown();
                    localInitialized = true;
                }
            } else if (dataChannel.isClosed()) {
                dataChannel = null;
            }
            return false;
        }
    }
}
