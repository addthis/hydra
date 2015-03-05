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
import javax.annotation.Nullable;

import java.io.File;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import java.nio.file.Paths;

import com.addthis.basis.util.Files;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.IndexField;
import com.addthis.codec.annotations.Time;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.source.QueryEngineSource;
import com.addthis.hydra.data.query.source.QueryEngineSourceSingle;
import com.addthis.hydra.data.tree.ReadTree;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceQuery extends TaskDataSource implements DataChannelOutput {

    private static final Logger log = LoggerFactory.getLogger(DataSourceQuery.class);

    private final BundleFormat format = new ListBundleFormat();

    /**
     * The tree job that is to be queried. Use
     * the literal {@code ${hydra.task.jobid}} for a job
     * to query itself. This field is required.
     */
    @Nonnull private final String jobId;

    /**
     * Optionally specify the directory
     * of the tree for the given job. Default
     * is "data".
     */
    @Nonnull private final String treeDir;

    /**
     * Path for the query. This field is required.
     */
    @Nonnull private final String path;

    /**
     * Operations for the query. This field is optional.
     */
    @Nullable private final String ops;

    /**
     * Optionally specify the field names for the columns
     * produced by the query. Otherwise the fields will
     * be named "0", "1", "2", etc. Default is null.
     */
    @Nullable private final String[] fields;

    /**
     * Maximum size of the queue that stores bundles prior to their processing.
     * Default is 128.
     */
    private final int buffer;

    @Time(TimeUnit.MILLISECONDS)
    private final int pollInterval;

    private final int pollCountdown;

    private final BlockingQueue<Bundle> queue;

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final AtomicReference<Throwable> firstError = new AtomicReference<>();

    private QueryEngineSourceSingle queryEngine;

    private QueryEngineSource.Handle queryHandle;

    private File tempDir;

    private boolean localInputSource;

    private IndexField[] fieldsIn;
    private AutoField[] fieldsOut;

    @JsonCreator
    public DataSourceQuery(@JsonProperty(value = "jobId", required = true) String jobId,
                           @JsonProperty(value = "treeDir", required = true) String treeDir,
                           @JsonProperty(value = "path", required = true) String path,
                           @JsonProperty("ops") String ops,
                           @JsonProperty("fields") String[] fields,
                           @JsonProperty("buffer") int buffer,
                           @JsonProperty("pollInterval") int pollInterval,
                           @JsonProperty("pollCountdown") int pollCountdown) {
        this.jobId = jobId;
        this.treeDir = treeDir;
        this.path = path;
        this.ops = ops;
        this.fields = fields;
        this.buffer = buffer;
        this.pollInterval = pollInterval;
        this.pollCountdown = pollCountdown;
        this.queue = new LinkedBlockingQueue<>(this.buffer);
    }

    @Override public void init() {
        localInputSource = Objects.equals(System.getenv("HYDRA_JOBID"), jobId);
        if (!localInputSource) {
            throw new IllegalStateException("At this time only queries to ${hydra.task.jobid} are supported");
        }
        if (fields != null) {
            fieldsIn = new IndexField[fields.length];
            fieldsOut = new AutoField[fields.length];
            for (int i = 0; i < fields.length; i++) {
                fieldsIn[i] = new IndexField(i);
                fieldsOut[i] = AutoField.newAutoField(fields[i]);
            }
        }
        try {
            File dir = Paths.get(treeDir).toFile();
            tempDir = Files.createTempDir();
            queryEngine = new QueryEngineSourceSingle(new ReadTree(dir));
            Query query = new Query(jobId, new String[] {path}, null);
            QueryOpProcessor proc = new QueryOpProcessor.Builder(this, new String[]{ops})
                    .tempDir(tempDir).build();
            queryHandle = queryEngine.query(query, proc);
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override public Bundle next() throws DataChannelError {
        int countdown = pollCountdown;
        while ((pollCountdown == 0) || (countdown-- > 0)) {
            Bundle next = pollAndCloseOnInterrupt(pollInterval, TimeUnit.MILLISECONDS);
            if (next != null) {
                return next;
            }
            if (closeFuture.isDone()) {
                closeFuture.join();
                return null;
            }
            if (pollCountdown > 0) {
                log.info("next polled null, retrying {} more times. shuttingDown={}", countdown, shuttingDown.get());
            }
            log.info("null poll ");
        }
        if (countdown < 0) {
            log.info("exit with no data during poll countdown");
        }
        return null;
    }

    private Bundle pollAndCloseOnInterrupt(long pollFor, TimeUnit unit) {
        boolean interrupted = false;
        try {
            long remainingNanos = unit.toNanos(pollFor);
            long end = System.nanoTime() + remainingNanos;
            while (true) {
                try {
                    return queue.poll(remainingNanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                    log.info("interrupted while polling for bundles; closing source then resuming poll");
                    close();
                    remainingNanos = end - System.nanoTime();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override public Bundle peek() throws DataChannelError {
        return queue.peek();
    }

    @Override public void close() {
        if (shuttingDown.compareAndSet(false, true)) {
            try {
                if (queryHandle != null && queryHandle.isAlive()) {
                    queryHandle.cancel("closing query input source");
                }
            } catch (Exception ex) {
                firstError.compareAndSet(null, ex);
                log.error("Error attempting to cancel query:", ex);
            }
            try {
                if (tempDir != null) {
                    Files.deleteDir(tempDir);
                }
            } catch (Exception ex) {
                firstError.compareAndSet(null, ex);
                log.error("Error attempting to delete temporary directory:", ex);
            }
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

    @Override public void send(Bundle input) {
        try {
            Bundle output = input;
            if (fields != null) {
                output = output.createBundle();
                for (int i = 0; i < fields.length; i++) {
                    IndexField fieldIn = fieldsIn[i];
                    AutoField fieldOut = fieldsOut[i];
                    fieldOut.setValue(output, fieldIn.getValue(input));
                }
            }
            queue.put(output);
        } catch (InterruptedException ex) {
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
        return format.createBundle();
    }
}
