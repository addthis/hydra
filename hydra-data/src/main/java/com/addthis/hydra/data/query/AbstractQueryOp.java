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

import java.io.IOException;

import java.util.List;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormatted;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueString;


public abstract class AbstractQueryOp implements QueryOp {

    public static final ValueLong ZERO = ValueFactory.create(0);
    public static final ValueString EMPTY_STRING = ValueFactory.create("");

    private QueryOp next;
    private QueryMemTracker memTracker;
    private BundleColumnBinder sourceBinder;

    public BundleColumnBinder getSourceColumnBinder(BundleFormatted row) {
        return getSourceColumnBinder(row, null);
    }

    public BundleColumnBinder getSourceColumnBinder(BundleFormatted row, String fields[]) {
        if (sourceBinder == null) {
            sourceBinder = new BundleColumnBinder(row, fields);
        }
        return sourceBinder;
    }

    @Override
    public void close() throws IOException {
        // next should be null at the output, which is an instance of ResultChannelOutput
        if (next != null) {
            next.close();
        }
    }

    @Override
    public void sendTable(DataTable table, QueryStatusObserver queryStatusObserver) {
        for (Bundle row : table) {
            if (queryStatusObserver.queryCompleted || queryStatusObserver.queryCancelled) {
                break;
            } else {
                send(row);
            }
        }
        sendComplete();
    }

    @Override
    public void send(List<Bundle> bundles) {
        if (bundles != null && !bundles.isEmpty()) {
            for (Bundle bundle : bundles) {
                send(bundle);
            }
        }
    }

    @Override
    public QueryMemTracker getMemTracker() {
        return memTracker;
    }

    @Override
    public void setNext(QueryMemTracker memTracker, QueryOp next) {
        this.memTracker = memTracker;
        this.next = next;
    }

    @Override
    public QueryOp getNext() {
        return next;
    }

    @Override
    public String getSimpleName() {
        return getClass().getSimpleName().toString();
    }

    @Override
    public String toString() {
        QueryOp op = this;
        StringBuffer sb = new StringBuffer();
        while (op != null) {
            if (op != this) {
                sb.append(" > ");
            }
            sb.append(op.getSimpleName());
            op = op.getNext();
        }
        return sb.toString();
    }

    public static int[] csvToInts(String args) {
        String sv[] = Strings.splitArray(args, ",");
        int ov[] = new int[sv.length];
        for (int i = 0; i < ov.length; i++) {
            ov[i] = Integer.parseInt(sv[i]);
        }
        return ov;
    }

    /**
     * @param field array of BundleField
     * @param row   source row
     * @return a compound key
     */
    public static String createCompoundKey(BundleField field[], Bundle row) {
        String key = null;
        for (BundleField kc : field) {
            String kv = ValueUtil.asNativeString(row.getValue(kc));
            if (kv == null) {
                continue;
            }
            if (key == null) {
                key = kv;
            } else {
                key = key.concat(kv);
            }
        }
        return key;
    }

    public class BundleMapConf<K extends Enum> {

        private K op;
        private BundleField from;
        private BundleField to;

        public K getOp() {
            return op;
        }

        public void setOp(K op) {
            this.op = op;
        }

        public BundleField getFrom() {
            return from;
        }

        public void setFrom(BundleField from) {
            this.from = from;
        }

        public BundleField getTo() {
            return to;
        }

        public void setTo(BundleField to) {
            this.to = to;
        }
    }
}
