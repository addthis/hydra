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

import java.util.Iterator;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.channel.BlockingBufferedConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class TestOp {

    private static final boolean debug = false;//true;

    public static void doOpTest(DataTable in, String ops, DataTable out, int formatSize) throws Exception {
        doOpTest(in, ops, out, formatSize, 0, 0);
    }

    public static void doOpTest(DataTable in, String ops, DataTable out, int formatSize, int tipRow, int tipMem) throws Exception {
        BlockingBufferedConsumer buffer = new BlockingBufferedConsumer();
        QueryOpProcessor qp = new QueryOpProcessor(buffer).setMemTip(tipMem).setRowTip(tipRow).parseOps(ops);
        for (Bundle row : in) {
            if (debug) {
                System.out.println("send " + row);
            }
            qp.send(row);
        }
        if (debug) {
            System.out.println("ops " + ops);
        }
        qp.sendComplete();
        Iterator<Bundle> got = buffer.getTable().iterator();
        Iterator<Bundle> expect = out.iterator();
        if (debug) {
            System.out.println("got=" + got + " expect=" + expect);
        }
        while (expect.hasNext()) {
            assertTrue("missing results", got.hasNext());
            Bundle gotNext = got.next();
            Bundle expectNext = expect.next();
            if (debug) {
                System.out.println("gotNext=" + gotNext + " expectNext=" + expectNext);
            }
            compareBundles(expectNext, gotNext);
            if (formatSize > 0) {
                assertEquals("format size was not correct", formatSize, gotNext.getFormat().getFieldCount());
            }
        }
        assertFalse("got hadNext() when it should not", got.hasNext());
        qp.close();
    }

    public static void doOpTest(DataTable in, String ops, DataTable out) throws Exception {
        doOpTest(in, ops, out, -1);
    }

    public static void doOpTest(DataTable in, String ops, DataTable out, int tipRow, int tipMem) throws Exception {
        doOpTest(in, ops, out, -1, tipRow, tipMem);
    }

    private static void compareBundles(Bundle expect, Bundle got) {
        for (BundleField field : expect.getFormat()) {
            ValueObject v1 = expect.getValue(field);
            ValueObject v2 = got.getValue(field);
            assertEquals(v1, v2);
        }
        assertEquals("column count mismatch", expect.getCount(), got.getCount());
    }

    public static DataTableHelper parse(String simple) {
        DataTableHelper t = new DataTableHelper();
        for (String row : Strings.splitArray(simple, "|")) {
            t.tr();
            for (String col : Strings.splitArray(row, " ")) {
                t.td(col);
            }
        }
        return t;
    }

}
