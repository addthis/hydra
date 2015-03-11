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
package com.addthis.hydra.data.query;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import java.util.List;

import java.nio.file.Path;

import com.addthis.basis.kv.KVPair;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.LessStrings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.hydra.data.util.BundleUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.util.concurrent.ImmediateEventExecutor;

/**
 * <pre>
 * ops=OP[;OP1;OP2;...]
 *
 * OP   := NAME=ARGS
 * NAME := avg|count|diff|... (see operator name constants below)
 * ARGS := operator specific argument string (see specific Op<NAME> class for argument string format)
 * </pre>
 * <p/>
 * <p/>
 * TODO memory limits need to be re-implemented
 * TODO see Query for other TODOs that need implementation / support here
 */
@NotThreadSafe
public class QueryOpProcessor implements DataChannelOutput, QueryMemTracker, Closeable {

    private static final Logger log = LoggerFactory.getLogger(QueryOpProcessor.class);

    private static final long   OP_TIPMEM           = Parameter.longValue("query.tipmem", 0);
    private static final long   OP_TIPROW           = Parameter.longValue("query.tiprow", 0);
    private static final long   OP_MAXROWS          = Parameter.longValue("query.max.rows", 0);
    private static final long   OP_MAXCELLS         = Parameter.longValue("query.max.cells", 0);
    private static final String TMP_SORT_DIR_STRING =
            Parameter.value("query.tmpdir", "query.tmpdir");

    private final long memTip;
    private final long rowTip;
    private final File tempDir;

    private final ChannelProgressivePromise opPromise;
    private final ResultChannelOutput       output;
    private final QueryMemTracker           memTracker;
    private final DataTableFactory          tableFactory;

    @Nonnull private QueryOp firstOp;
    private QueryOp lastOp;
    private long    rowsin;
    private long    cellsin;

    private QueryOpProcessor(Builder builder) {
        this(builder.output, builder.queryPromise, builder.tempDir,
             builder.memTip, builder.rowTip, builder.memTracker, builder.ops);
    }

    public QueryOpProcessor(DataChannelOutput output, String[] ops) {
        this(new Builder(output, ops));
    }

    public QueryOpProcessor(
            DataChannelOutput output,
            String[] ops,
            ChannelProgressivePromise opPromise) {
        this(new Builder(output, ops).queryPromise(opPromise));
    }

    public QueryOpProcessor(
            DataChannelOutput output, ChannelProgressivePromise opPromise,
            File tempDir, long memTip, long rowTip, QueryMemTracker memTracker, String[] ops) {
        this.opPromise = opPromise;
        this.tempDir = tempDir;
        this.memTip = memTip;
        this.rowTip = rowTip;
        this.output = new ResultChannelOutput(output, opPromise);
        this.memTracker = memTracker;
        this.firstOp = this.output;
        this.tableFactory = new DataTableFactory() {
            @Override
            public DataTable createTable(int expectedSize) {
                try {
                    return new ResultTableTuned(QueryOpProcessor.this.tempDir,
                                                QueryOpProcessor.this.rowTip,
                                                QueryOpProcessor.this.memTip, this, expectedSize);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        parseOps(ops);
    }

    public ChannelProgressivePromise opPromise() {
        return opPromise;
    }

    public DataTableFactory tableFactory() {
        return tableFactory;
    }

    public Path tempDir() {
        return tempDir.toPath();
    }

    public long rowTip() {
        return rowTip;
    }

    public long memTip() {
        return memTip;
    }

    public String printOps() {
        return firstOp.toString();
    }

    @Nullable public static QueryOp generateOps(QueryOpProcessor processor,
                                                ChannelProgressivePromise promise,
                                                QueryOp tail,
                                                String... opslist) {

        if ((opslist == null) || (opslist.length == 0)) {
            return null;
        }

        QueryOp firstOp = null;
        QueryOp lastOp = null;

        /* remaining ops stack is processed in reverse order */
        for (int i = opslist.length - 1; i >= 0; i--) {
            String ops = opslist[i];
            if (ops == null) {
                continue;
            }

            for (String s : LessStrings.split(ops, ";")) {
                KVPair kv = KVPair.parsePair(s);
                String args = kv.getValue();
                String opName = kv.getKey().toUpperCase();
                Op op = Op.valueOf(opName);
                if (op == null) {
                    throw new RuntimeException("unknown op : " + kv);
                }
                QueryOp newOp = op.build(processor, args, promise);
                if (lastOp == null) {
                    firstOp = newOp;
                } else {
                    lastOp.setNext(processor, newOp);
                }
                lastOp = newOp;
                newOp.setNext(processor, tail);
            }
        }
        return firstOp;
    }

    private void parseOps(String... opslist) {
        QueryOp newFirstOp = generateOps(this, opPromise, output, opslist);
        // follow the query operations to the lastOp
        if (newFirstOp != null) {
            firstOp = newFirstOp;
            QueryOp current = firstOp;
            while (current.getNext() != output) {
                current = current.getNext();
            }
            lastOp = current;
        }
    }

    public QueryOpProcessor appendOp(QueryOp op) {
        if (lastOp == null) {
            firstOp = op;
        } else {
            lastOp.setNext(this, op);
        }
        lastOp = op;
        op.setNext(this, output);
        return this;
    }

    @Override
    public String toString() {
        return "RP[memtip=" + memTip + ",rowtip=" + rowTip + ",rows=" + rowsin + ",cells=" + cellsin + "]";
    }

    public void processRow(Bundle row) throws QueryException {
        rowsin++;
        cellsin += row.getCount();
        if ((opPromise != null) && !opPromise.isDone()) {
            firstOp.send(row);
        }
        if ((OP_MAXROWS > 0) && (rowsin > OP_MAXROWS)) {
            throw new QueryException("query exceeded max input rows: " + OP_MAXROWS);
        }
        if ((OP_MAXCELLS > 0) && (cellsin > OP_MAXCELLS)) {
            throw new QueryException("query exceeded max input cells: " + OP_MAXCELLS);
        }
    }

    /**
     * batch append a result set as opposed to a single row
     */
    public void processResults(DataTable addresults) throws QueryException {
        Thread currentThread = Thread.currentThread();
        for (Bundle line : addresults) {
            if (currentThread.isInterrupted()) {
                /*
                 * clear interrupt. we don't do this above because interrupted()
                 * is a static method that looks up the current thread each
                 * time. we cache the thread and avoid this. there is no public
                 * accessor to clear the interrupt via isInterupted(boolean) for
                 * unknown reasons, thus this hack.
                 */
                Thread.interrupted();
                throw new QueryException("query interrupted");
            }
            processRow(line);
        }
    }

    public long getInputRows() {
        return rowsin;
    }

    public long getInputCells() {
        return cellsin;
    }

    @Override
    public void send(Bundle row) throws DataChannelError {
        processRow(row);
    }

    @Override
    public void send(List<Bundle> bundles) throws QueryException {
        if ((bundles != null) && !bundles.isEmpty()) {
            for (Bundle bundle : bundles) {
                processRow(bundle);
            }
        }
    }

    @Override
    public void sendComplete() {
        try {
            // anyone who sets the opPromise to success is responsible for ensuring query completion
            if (!opPromise.isDone()) {
                firstOp.sendComplete();
                opPromise.trySuccess();
            } else if (opPromise.isSuccess()) {
                log.debug("skipping sendComplete because opPromise was already complete");
            } else {
                log.debug("skipping sendComplete because opPromise was already failed");
            }
        } catch (Exception e) {
            log.debug("Exception while processing sendComplete on op processor");
            sourceError(BundleUtils.promoteHackForThrowables(e));
        } catch (Throwable e) {
            // hopefully an "out of off heap/direct memory" error
            log.error("Error while processing sendComplete on op processor");
            sourceError(BundleUtils.promoteHackForThrowables(e));
        }
    }

    /** Closes each QueryOp in turn and supresses exceptions. */
    @Override
    public void close() {
        QueryOp currentOp = firstOp;
        while (currentOp != null) {
            try {
                currentOp.close();
            } catch (Throwable ex) {
                // hopefully an "out of off heap/direct memory" error if not an exception
                log.error("unexpected exception or error while closing query op", ex);
            }
            currentOp = currentOp.getNext();
        }
    }

    @Override
    public void sourceError(Throwable er) {
        opPromise.tryFailure(er);
        output.getOutput().sourceError(er);
    }

    @Override
    public Bundle createBundle() {
        return output.getOutput().createBundle();
    }

    @Override
    public void trackBundle(Bundle bundle) {
        if (memTracker != null) {
            memTracker.trackBundle(bundle);
        }
    }

    @Override
    public void untrackBundle(Bundle bundle) {
        if (memTracker != null) {
            memTracker.untrackBundle(bundle);
        }
    }

    @Override
    public void untrackAllBundles() {
        if (memTracker != null) {
            memTracker.untrackAllBundles();
        }
    }

    public static final class Builder {

        private final DataChannelOutput output;
        private final String[] ops;

        private ChannelProgressivePromise queryPromise =
                new DefaultChannelProgressivePromise(null, ImmediateEventExecutor.INSTANCE);
        private long memTip = OP_TIPMEM;
        private long rowTip = OP_TIPROW;
        private File tempDir = new File(TMP_SORT_DIR_STRING);
        private QueryMemTracker memTracker = null;

        public Builder(DataChannelOutput output, String... ops) {
            this.output = output;
            this.ops = ops;
        }

        public Builder memTracker(QueryMemTracker memTracker) {
            this.memTracker = memTracker;
            return this;
        }

        public Builder memTip(long memTip) {
            this.memTip = memTip;
            return this;
        }

        public Builder rowTip(int rowTip) {
            this.rowTip = rowTip;
            return this;
        }

        public Builder tempDir(File tempDir) {
            this.tempDir = tempDir;
            return this;
        }

        public Builder queryPromise(ChannelProgressivePromise queryPromise) {
            this.queryPromise = queryPromise;
            return this;
        }

        public QueryOpProcessor build() {
            return new QueryOpProcessor(this);
        }
    }
}
