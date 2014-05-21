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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.addthis.basis.util.Bytes;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueString;
import com.addthis.hydra.data.query.AbstractRowOp;
import com.addthis.maljson.JSONObject;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">maps values within a
 * column to new values with regular expression support</span>.
 * <p/>
 * <p>The syntax for this operation is "rmap=N::{column map}". N is the number of the column on which
 * transformations will be applied. {column map} is a JSON formatted map object. For each value in the
 * N<sup>th</sup> column: if that value is present in the JSON map then that value in the output
 * is replaced with corresponding value in the JSON map. Transformations are made in-place on the
 * data rows.
 * <p>Example: rmap=1::{"widget..\.png" : "widgetXX.png"}
 * <p>Warning: Comparing a series of regular expressions is *much*
 * slower than the single hash in {@link OpMap map}.
 *
 * @user-reference
 * @hydra-name rmap
 */
public class OpRMap extends AbstractRowOp {

    private int col;
    // todo: make linked?
    private Map<Pattern, ValueString> map;

    public OpRMap(String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        try {
            String[] opt = args.split("::");
            if (opt.length >= 2) {
                col = Integer.parseInt(opt[0]);
                map = new HashMap<>();
                JSONObject jo = new JSONObject(Bytes.urldecode(opt[1]));
                for (String key : jo.keySet()) {
                    map.put(Pattern.compile(key), ValueFactory.create(jo.optString(key)));
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Bundle rowOp(Bundle row) {
        if (col < row.getCount()) {
            BundleColumnBinder binder = getSourceColumnBinder(row);
            ValueObject oldval = binder.getColumn(row, col);
            if (oldval != null) {
                for (Map.Entry<Pattern, ValueString> entry : map.entrySet()) {
                    if (entry.getKey().matcher(oldval.toString()).matches()) {
                        binder.setColumn(row, col, entry.getValue());
                        //break;
                    }
                }
            }
        }
        return row;
    }
}
