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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import java.nio.file.Paths;

import com.addthis.basis.util.Files;

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.util.AutoField;
import com.addthis.hydra.data.query.Query;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.source.QueryEngineSource;
import com.addthis.hydra.data.query.source.QueryEngineSourceSingle;
import com.addthis.hydra.data.tree.ReadTree;

import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceQuery extends AbstractStreamDataSource implements DataChannelOutput {

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
     * Optionally specify the field names for the columns
     * produced by the query. Otherwise the fields will
     * be named "0", "1", "2", etc. Default is null.
     */
    @JsonProperty private AutoField[] fields;

    private final AtomicReference<Throwable> firstError = new AtomicReference<>();

    private QueryEngineSourceSingle queryEngine;

    private QueryEngineSource.Handle queryHandle;

    private File tempDir;

    private boolean localInputSource;

    @Override public void init() {
        super.init();
        localInputSource = (jobId == null);
        if (!localInputSource) {
            throw new UnsupportedOperationException("At this time only local queries are supported");
        }
        try {
            File dir = Paths.get(treeDir).toFile();
            tempDir = Files.createTempDir();
            queryEngine = new QueryEngineSourceSingle(new ReadTree(dir));
            Query query = new Query(System.getenv("HYDRA_JOBID"), new String[] {path}, null);
            QueryOpProcessor proc = new QueryOpProcessor.Builder(this, new String[]{ops})
                    .tempDir(tempDir).build();
            queryHandle = queryEngine.query(query, proc);
            initialized.countDown();
            localInitialized = true;
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
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

    private String[] fieldNames(Bundle bundle) {
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
}
