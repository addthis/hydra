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
package com.addthis.hydra.data.query.op;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.hydra.data.query.AbstractTableOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">calculates disorder values</span>.
 * <p/>
 * <p>Table-level operation that involves a primary key, a secondary key, and
 * optionally a frequency column.  For each primary key, computes how "disorderly" the
 * set of secondary keys is. If the frequency column is omitted then each row is
 * assumed to have a weight of 1.
 * <p/>
 * <pre>a b 1
 * a c 2
 * a d 1
 * b x 2
 * b y 3
 * b d 1
 * <p/>
 * disorder=0:1:2
 * <p/>
 * a 0.451544993496 0.625
 * b 0.439247291136 0.611111111111</pre>
 *
 * @user-reference
 * @hydra-name disorder
 */
public class OpDisorder extends AbstractTableOp {

    public static final Long ONE = Long.valueOf(1);

    private int primary;
    private int secondary;
    private int frequency;

    public OpDisorder(DataTableFactory tableFactory, String args, ChannelProgressivePromise queryPromise) {
        super(tableFactory, queryPromise);
        String[] split = args.split(":");
        if (split.length < 2 || split.length > 3) {
            throw new IllegalArgumentException("expected disorder=p:s[:f], got " + args);
        }

        primary = Integer.parseInt(split[0]);
        secondary = Integer.parseInt(split[1]);
        if (split.length == 3) {
            frequency = Integer.parseInt(split[2]);
        } else {
            frequency = -1;
        }
    }

    @Override
    public DataTable tableOp(DataTable input) {
        int max = Math.max(primary, Math.max(secondary, frequency));
        Map<String, Map<String, Long>> data = new TreeMap<>();
        BundleColumnBinder binder = getSourceColumnBinder(input);

        for (Bundle row : input) {
            if (row.getCount() < max) {
                continue;
            }

            String p = binder.getColumn(row, primary).toString();
            if (p == null) {
                p = "";
            }

            String s = binder.getColumn(row, secondary).toString();
            if (s == null) {
                s = "";
            }

            Long f = frequency < 0 ? ONE : ValueUtil.asNumberOrParse(binder.getColumn(row, frequency)).asLong().getLong();
            if (f == null || f.longValue() <= 0) {
                continue;
            }

            bump(data, p, s, f);
        }

        DataTable output = createTable(data.size());

        for (String key : data.keySet()) {
            Bundle row = output.createBundle();
            binder.appendColumn(row, ValueFactory.create(key));
            for (double d : computeDisorder(data.get(key))) {
                binder.appendColumn(row, ValueFactory.create(d));
            }
        }

        return output;
    }

    // data[p][s] += f
    protected static void bump(Map<String, Map<String, Long>> data, String p, String s, long f) {
        Map<String, Long> m = data.get(p);
        if (m == null) {
            data.put(p, m = new HashMap<>());
        }

        if (m.containsKey(s)) {
            m.put(s, Long.valueOf(f + m.get(s).longValue()));
        } else {
            m.put(s, Long.valueOf(f));
        }
    }

    public static double[] computeDisorder(Map<String, Long> data) {
        double sum = 0.0;
        double ent = 0.0;
        double gin = 0.0;

        for (String k : data.keySet()) {
            sum += data.get(k).doubleValue();
        }

        for (String k : data.keySet()) {
            double prk = data.get(k).doubleValue() / sum;

            ent += (prk * Math.log10(prk));

            for (String k2 : data.keySet()) {
                if (k != k2) {
                    double prk2 = data.get(k2).doubleValue() / sum;
                    gin += prk * prk2;
                }
            }
        }

        return new double[]{-1.0 * ent, gin};
    }

    public static void main(String[] args) throws Exception {
        Map<String, Long> data = new HashMap<>();
        data.put("a", 1L);
        data.put("b", 2L);
        data.put("c", 2L);
        System.err.println(data);
        System.err.println(Arrays.toString(computeDisorder(data)));
        System.err.println();

        data = new HashMap<>();
        data.put("a", 1L);
        data.put("b", 2L);
        data.put("c", 3L);
        System.err.println(data);
        System.err.println(Arrays.toString(computeDisorder(data)));
        System.err.println();
    }
}
