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

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.table.DataTable;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.query.AbstractTableOp;
import com.addthis.hydra.data.query.QueryElement;
import com.addthis.hydra.data.query.QueryEngine;
import com.addthis.hydra.data.query.QueryOp;
import com.addthis.hydra.data.query.QueryOpProcessor;
import com.addthis.hydra.data.query.QueryStatusObserver;
import com.addthis.hydra.data.tree.DataTree;
import com.addthis.hydra.data.tree.TreeNodeList;
import com.addthis.hydra.task.output.ValuesOutput;

import org.apache.commons.lang3.mutable.MutableInt;

/**
 * @user-reference
 * @hydra-name output
 */
public class PathOutput extends PathElement {

    /**
     * If non-null then use this value in the
     * {@link Object#toString() toString()} method. Default is null.
     */
    @Codec.Set(codable = true)
    private String description;

    @Codec.Set(codable = true)
    private QueryElement[] query;

    @Codec.Set(codable = true)
    private QueryOp[] ops;

    @Codec.Set(codable = true)
    private String queryString;

    @Codec.Set(codable = true)
    private String opsString;

    /**
     * This field is required.
     */
    @Codec.Set(codable = true, required = true)
    private ValuesOutput output;

    @Codec.Set(codable = true)
    private int maxmem;

    @Codec.Set(codable = true)
    private boolean debug;

    private QueryEngine engine;

    @Override
    public String toString() {
        return description != null ? description : Strings.join(query, "/");
    }

    @Override
    public void resolve(final TreeMapper mapper) {
        super.resolve(mapper);
        if (queryString == null && query == null) {
            throw new RuntimeException("either query or queryString required in PathOutput");
        }
        if (query == null && queryString != null) {
            String q[] = Strings.splitArray(queryString, "/");
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
            engine.search(query, rp);
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
            super(processor, processor.getQueryStatusObserver());
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
