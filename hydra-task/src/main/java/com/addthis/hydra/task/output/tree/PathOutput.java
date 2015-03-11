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
package com.addthis.hydra.task.output.tree;

import com.addthis.basis.util.LessStrings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.table.DataTable;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.query.AbstractTableOp;
import com.addthis.hydra.data.query.QueryElement;
import com.addthis.hydra.data.query.QueryOp;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.engine.QueryEngine;
import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.TreeNodeList;
import com.addthis.hydra.task.output.ValuesOutput;

import org.apache.commons.lang3.mutable.MutableInt;

import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.util.concurrent.ImmediateEventExecutor;

/**
 * @user-reference
 */
public class PathOutput extends PathElement {

    /**
     * If non-null then use this value in the
     * {@link Object#toString() toString()} method. Default is null.
     */
    @FieldConfig(codable = true)
    private String description;

    @FieldConfig(codable = true)
    private QueryElement[] query;

    @FieldConfig(codable = true)
    private QueryOp[] ops;

    @FieldConfig(codable = true)
    private String queryString;

    @FieldConfig(codable = true)
    private String opsString;

    /**
     * This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private ValuesOutput output;

    @FieldConfig(codable = true)
    private int maxmem;

    @FieldConfig(codable = true)
    private boolean debug;

    private QueryEngine engine;

    @Override
    public String toString() {
        return description != null ? description : LessStrings.join(query, "/");
    }

    @Override
    public void resolve(final TreeMapper mapper) {
        super.resolve(mapper);
        if ((queryString == null) && (query == null)) {
            throw new RuntimeException("either query or queryString required in PathOutput");
        }
        if (query == null) {
            String[] q = LessStrings.splitArray(queryString, "/");
            query = new QueryElement[q.length];
            MutableInt col = new MutableInt(0);
            int i = 0;
            for (String qe : q) {
                query[i++] = new QueryElement().parse(qe, col);
            }
        }
    }

    @Override
    public TreeNodeList getNextNodeList(final TreeMapState state) {
        exec(state.current().getTreeRoot());
        return TreeMapState.empty();
    }

    public void exec(DataTree tree) {
        synchronized (this) {
            if (engine == null) {
                engine = new QueryEngine(tree);
            }
        }
        QueryOpProcessor rp = new QueryOpProcessor.Builder(output, opsString)
                .memTip(maxmem).build();
        if (ops != null) {
            for (QueryOp op : ops) {
                rp.appendOp(op);
            }
        }
        rp.appendOp(new OutputOp(rp));
        try {
            output.open();
            engine.search(query, rp,
                    new DefaultChannelProgressivePromise(null, ImmediateEventExecutor.INSTANCE));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            output.sendComplete();
        }
    }

    /**
     * term op chain with this
     */
    class OutputOp extends AbstractTableOp {

        public OutputOp(QueryOpProcessor processor) {
            super(processor.tableFactory(), processor.opPromise());
        }

        @Override
        public void send(Bundle row) {
            try {
                if (debug) {
                    log.warn("--> " + row);
                }
                output.send(row);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public DataTable tableOp(DataTable result) {
            return null;
        }
    }
}
