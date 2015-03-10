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

import com.addthis.basis.util.MemoryCounter;
import com.addthis.basis.util.LessStrings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormatted;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueString;

import io.netty.channel.ChannelProgressivePromise;


public abstract class AbstractQueryOp implements QueryOp {

    public static final ValueLong   ZERO         = ValueFactory.create(0);
    public static final ValueString EMPTY_STRING = ValueFactory.create("");

    @MemoryCounter.Mem(estimate = false)
    private QueryOp            next;
    @MemoryCounter.Mem(estimate = false)
    private QueryMemTracker    memTracker;
    private BundleColumnBinder sourceBinder;

    @MemoryCounter.Mem(estimate = false)
    protected final ChannelProgressivePromise opPromise;

    protected AbstractQueryOp(ChannelProgressivePromise opPromise) {
        this.opPromise = opPromise;
    }

    public BundleColumnBinder getSourceColumnBinder(BundleFormatted row) {
        return getSourceColumnBinder(row, null);
    }

    public BundleColumnBinder getSourceColumnBinder(BundleFormatted row, String[] fields) {
        if (sourceBinder == null) {
            sourceBinder = new BundleColumnBinder(row, fields);
        }
        return sourceBinder;
    }

    @Override public ChannelProgressivePromise getOpPromise() {
        return opPromise;
    }

    @Override
    public void close() throws IOException {
        // sub-classes should implement for any clean-up they need
    }

    @Override
    public void sendTable(DataTable table) {
        for (Bundle row : table) {
            if (opPromise.isDone()) {
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
        String[] sv = LessStrings.splitArray(args, ",");
        int[] ov = new int[sv.length];
        for (int i = 0; i < ov.length; i++) {
            ov[i] = Integer.parseInt(sv[i]);
        }
        return ov;
    }

    public static float[] csvToFloats(String args) {
        String[] sv = LessStrings.splitArray(args, ",");
        float[] ov = new float[sv.length];
        for (int i = 0; i < ov.length; i++) {
            ov[i] = Float.parseFloat(sv[i]);
        }
        return ov;
    }

    /**
     * @param field array of BundleField
     * @param row   source row
     * @return a compound key
     */
    public static String createCompoundKey(BundleField[] field, Bundle row) {
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

}
