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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.addthis.basis.kv.KVPair;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.hydra.data.query.op.OpChangePoints;
import com.addthis.hydra.data.query.op.OpCompare;
import com.addthis.hydra.data.query.op.OpContains;
import com.addthis.hydra.data.query.op.OpDateFormat;
import com.addthis.hydra.data.query.op.OpDePivot;
import com.addthis.hydra.data.query.op.OpDiff;
import com.addthis.hydra.data.query.op.OpDiskSort;
import com.addthis.hydra.data.query.op.OpDisorder;
import com.addthis.hydra.data.query.op.OpFill;
import com.addthis.hydra.data.query.op.OpFold;
import com.addthis.hydra.data.query.op.OpFrequencyTable;
import com.addthis.hydra.data.query.op.OpGather;
import com.addthis.hydra.data.query.op.OpHistogram;
import com.addthis.hydra.data.query.op.OpLimit;
import com.addthis.hydra.data.query.op.OpMap;
import com.addthis.hydra.data.query.op.OpMedian;
import com.addthis.hydra.data.query.op.OpMerge;
import com.addthis.hydra.data.query.op.OpNoDup;
import com.addthis.hydra.data.query.op.OpNumber;
import com.addthis.hydra.data.query.op.OpOrder;
import com.addthis.hydra.data.query.op.OpOrderMap;
import com.addthis.hydra.data.query.op.OpPercentileDistribution;
import com.addthis.hydra.data.query.op.OpPercentileRank;
import com.addthis.hydra.data.query.op.OpPivot;
import com.addthis.hydra.data.query.op.OpRMap;
import com.addthis.hydra.data.query.op.OpRandomFail;
import com.addthis.hydra.data.query.op.OpRange;
import com.addthis.hydra.data.query.op.OpRemoveSingletons;
import com.addthis.hydra.data.query.op.OpReverse;
import com.addthis.hydra.data.query.op.OpRoll;
import com.addthis.hydra.data.query.op.OpSeen;
import com.addthis.hydra.data.query.op.OpSkip;
import com.addthis.hydra.data.query.op.OpSleep;
import com.addthis.hydra.data.query.op.OpString;
import com.addthis.hydra.data.query.op.OpTitle;
import com.addthis.hydra.data.query.op.OpTranspose;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * <pre>
 * ops=OP[;OP1;OP2;...]
 *
 * OP   := NAME=ARGS
 * NAME := avg|count|diff|... (see operator name constants below)
 * ARGS := operator specific argument string (see specific Op<NAME> class for argument string format)
 * </pre>
 *
 *         <p/>
 *         TODO memory limits need to be re-implemented
 *         TODO see Query for other TODOs that need implementation / support here
 */
public class QueryOpProcessor implements DataChannelOutput, DataTableFactory, QueryMemTracker, Closeable {

    private static final Logger log = LoggerFactory.getLogger(QueryOpProcessor.class);
    private static final Map<String, OPS> opmap = new HashMap<String, OPS>();
    /* this forces the jvm to compile/eval OPS which is required for the switch */
    private static final OPS NullOpType = OPS.NULL;

    private static final int maxRows = Parameter.intValue("query.max.rows", 0);
    private static final int maxCells = Parameter.intValue("query.max.cells", 0);

    private enum OPS {
        AVG("avg"),
        BLOOM("bloom"),
        CHANGEPOINTS("changepoints"),
        COMPARE("compare"),
        CONTAINS("contains"),
        COUNT("count"),
        DATEF("datef"),
        DSORT("dsort"),
        DIFF("diff"),
        DEPIVOT("depivot"),
        DISORDER("disorder"),
        DELTA("delta"),
        DOMAIN("domain"),
        FOLD("fold"),
        FREQUENCYTABLE("ftable"),
        GATHER("gather"),
        FILL("fill"),
        HISTOGRAM("histo"),
        DISTRIBUTION("distribution"),
        LIMIT("limit"),
        MAP("map"),
        RMAP("rmap"),
        MAX("max"),
        MEDIAN("median"),
        MERGE("merge"),
        MIN("min"),
        NUMBER(new String[]{"num", "math"}),
        NODUP("nodup"),
        NULL("null"),
        ORDER("order"),
        ORDERMAP("ordermap"),
        PAD("pad"),
        PERCENTRANK("percentrank"),
        PIVOT("pivot"),
        RANGE("range"),
        REVERSE("reverse"),
        RMSING("rmsing"),
        RND_FAIL("rndfail"),
        SEEN("seen"),
        SKIP("skip"),
        SLEEP("sleep"),
        SORT("sort"),
        STRING("str"),
        SUM("sum"),
        TOP("top"),
        TITLE("title"),
        TRANSPOSE(new String[]{"trans", "t"});

        private OPS(String token) {
            opmap.put(token, this);
        }

        private OPS(String tokens[]) {
            for (String token : tokens) {
                opmap.put(token, this);
            }
        }
    }

    private QueryOp firstOp;
    private QueryOp lastOp;
    private long rowsin;
    private long cellsin;

    private long memTip = Query.tipMem;
    private int rowTip = Query.tipRow;
    private File tempDir = new File("query.tmp");
    private ResultChannelOutput output;
    private QueryMemTracker memTracker;
    private LinkedList<ResultTableTuned> toBeFreed = new LinkedList<ResultTableTuned>();

    private QueryStatusObserver queryStatusObserver = null;
    private Query query = null;

    public QueryOpProcessor(DataChannelOutput output, QueryStatusObserver queryStatusObserver) {
        this.queryStatusObserver = queryStatusObserver;
        this.output = new ResultChannelOutput(output);
        firstOp = this.output;
    }

    public QueryOpProcessor(DataChannelOutput output) {
        this.queryStatusObserver = new QueryStatusObserver();
        this.output = new ResultChannelOutput(output);
        firstOp = this.output;
    }

    public QueryStatusObserver getQueryStatusObserver() {
        return this.queryStatusObserver;
    }

    public QueryOpProcessor setQuery(Query query) {
        this.query = query;
        return this;
    }

    public String printOps() {
        return firstOp.toString();
    }

    public void setMemoryManager(QueryMemManager memManager) {
        if (memTracker != null) {
            memTracker.untrackAllBundles();
        }
        memTracker = memManager.allocateTracker();
    }

    public QueryOpProcessor setTempDir(File tempDir) {
        this.tempDir = tempDir;

        return this;
    }

    public QueryOpProcessor setMemTip(long bytemax) {
        this.memTip = bytemax;
        return this;
    }

    public QueryOpProcessor setRowTip(int tip) {
        rowTip = tip;
        return this;
    }

    public QueryOpProcessor parseOps(String ops) {
        if (ops == null) {
            return this;
        }

        for (String s : Strings.split(ops, ";")) {
            KVPair kv = KVPair.parsePair(s);
            String args = kv.getValue();
            OPS op = opmap.get(kv.getKey());
            if (op == null) {
                throw new RuntimeException("unknown op : " + kv);
            }
            switch (op) {
                case AVG:
                    appendOp(new OpRoll.AvgOpRoll(args));
                    break;
                case CHANGEPOINTS:
                    appendOp(new OpChangePoints(this, args));
                    break;
                case COMPARE:
                    appendOp(new OpCompare(args));
                    break;
                case CONTAINS:
                    appendOp(new OpContains(args));
                    break;
                case DATEF:
                    appendOp(new OpDateFormat(args));
                    break;
                case DELTA:
                    appendOp(new OpRoll.DeltaOpRoll(args));
                    break;
                case DEPIVOT:
                    appendOp(new OpDePivot(this, args));
                    break;
                case DIFF:
                    appendOp(new OpDiff(this, args));
                    break;
                case DISORDER:
                    appendOp(new OpDisorder(this, args));
                    break;
                case DSORT:
                    appendOp(new OpDiskSort(args, query, queryStatusObserver));
                    break;
                case FILL:
                    appendOp(new OpFill(args));
                    break;
                case FOLD:
                    appendOp(new OpFold(args));
                    break;
                case FREQUENCYTABLE:
                    appendOp(new OpFrequencyTable(this, args));
                    break;
                case GATHER:
                    appendOp(new OpGather(args, memTip, rowTip, tempDir.getPath(), queryStatusObserver));
                    break; // TODO move OpTop code into OpGather and delete OpTop
                case HISTOGRAM:
                    appendOp(new OpHistogram(args));
                    break;
                case DISTRIBUTION:
                    appendOp(new OpPercentileDistribution(this, args));
                    break;
                case LIMIT:
                    appendOp(new OpLimit(args, queryStatusObserver));
                    break;
                case MAP:
                    appendOp(new OpMap(args));
                    break;
                case RMAP:
                    appendOp(new OpRMap(args));
                    break;
                case MAX:
                    appendOp(new OpRoll.MaxOpRoll(args));
                    break;
                case MEDIAN:
                    appendOp(new OpMedian(this));
                    break;
                case MERGE:
                    appendOp(new OpMerge(args, queryStatusObserver));
                    break;
                case MIN:
                    appendOp(new OpRoll.MinOpRoll(args));
                    break;
                case NODUP:
                    appendOp(new OpNoDup());
                    break;
                case NUMBER:
                    appendOp(new OpNumber(args));
                    break;
                case ORDER:
                    appendOp(new OpOrder(args));
                    break;
                case ORDERMAP:
                    appendOp(new OpOrderMap(args));
                    break;
                case PAD:
                    appendOp(new OpFill(args, true));
                    break;
                case PERCENTRANK:
                    appendOp(new OpPercentileRank(this, args));
                    break;
                case PIVOT:
                    appendOp(new OpPivot(this, args));
                    break;
                case RANGE:
                    appendOp(new OpRange(this, args));
                    break;
                case REVERSE:
                    appendOp(new OpReverse(this));
                    break;
                case RMSING:
                    appendOp(new OpRemoveSingletons(this, args));
                    break;
                case RND_FAIL:
                    appendOp(new OpRandomFail(args));
                    break;
                case SEEN:
                    appendOp(new OpSeen(this, args));
                    break;
                case SKIP:
                    appendOp(new OpSkip(args));
                    break;
                case SLEEP:
                    appendOp(new OpSleep(args));
                    break;
                case SORT:
//                  appendOp(new OpSort(this, args));
                    appendOp(new OpDiskSort(args, query, queryStatusObserver));
                    break;
                case STRING:
                    appendOp(new OpString(args));
                    break;
                case SUM:
                    appendOp(new OpRoll.SumOpRoll(args));
                    break;
                case TITLE:
                    appendOp(new OpTitle(args));
                    break;
                case TOP:
                    appendOp(new OpGather(args, memTip, rowTip, tempDir.getPath(), queryStatusObserver));
                    break;
                case TRANSPOSE:
                    appendOp(new OpTranspose(this));
                    break;
            }
        }

        return this;
    }

    @Override
    public String toString() {
        return "RP[memtip=" + memTip + ",rowtip=" + rowTip + ",rows=" + rowsin + ",cells=" + cellsin + "]";
    }

    /**
     * @param op
     */
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

    /**
     * @param row
     */
    public void processRow(Bundle row) throws QueryException {
        /**
         * sync b/c multiple threads expected to call this in workers, maybe
         * masters
         */
        synchronized (firstOp) {
            rowsin++;
            cellsin += row.getCount();
            if (queryStatusObserver != null && !queryStatusObserver.queryCompleted) {
                firstOp.send(row);
            }
        }
        if (maxRows > 0 && rowsin > maxRows) {
            throw new QueryException("query exceeded max input rows: " + maxRows);
        }
        if (maxCells > 0 && cellsin > maxCells) {
            throw new QueryException("query exceeded max input cells: " + maxCells);
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
    public DataTable createTable(int sizeHint) {
        try {
            ResultTableTuned result = new ResultTableTuned(tempDir, rowTip, memTip, this, sizeHint);
            toBeFreed.add(result);
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void send(Bundle row) throws DataChannelError {
        processRow(row);
    }

    @Override
    public void send(List<Bundle> bundles) throws QueryException {
        if (bundles != null && !bundles.isEmpty()) {
            for (Bundle bundle : bundles) {
                processRow(bundle);
            }
        }
    }

    @Override
    public void sendComplete() {
        synchronized (firstOp) {
            firstOp.sendComplete();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (firstOp) {
            firstOp.close();
        }
    }

    @Override
    public void sourceError(DataChannelError er) {
        output.getOutput().sourceError(er);
        try {
            close();
        } catch (IOException e) {
            log.warn("Exception while closing QueryOpProcessor", e);
            throw new RuntimeException(e);
        }
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
}
