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

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.hydra.data.query.AbstractRowOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <p>This query operation <span class="hydra-summary">reorders columns</span>.
 * <p/>
 * <p>The syntax for this operation is order=X,Y,Z,etc. Where X, Y, Z are the existing
 * column numbers. The output columns will be reordering according to the relative
 * ordering of X, Y, Z, etc. Any columns not specified in the operation will be dropped.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *
 * A 1 art
 * B 2 bot
 * C 3 cog
 * D 4 din
 *
 * order=1,2,0
 *
 * 1 art A
 * 2 bot B
 * 3 cog C
 * 4 din D
 * </pre>
 *
 * @user-reference
 * @hydra-name order
 */
public class OpOrder extends AbstractRowOp {

    private final ListBundleFormat format = new ListBundleFormat();
    private final String[] fields;
    private BundleColumnBinder mapIn;
    private BundleColumnBinder mapOut;

    public OpOrder(String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        fields = Strings.splitArray(args, ",");
    }

    @Override
    public Bundle rowOp(Bundle row) {
        Bundle next = new ListBundle(format);
        if (mapIn == null) {
            mapIn = new BundleColumnBinder(row, fields);
            mapOut = new BundleColumnBinder(next, fields);
        }
        BundleField[] fieldsIn = mapIn.getFields();
        BundleField[] fieldsOut = mapOut.getFields();
        for (int i = 0; i < fields.length; i++) {
            next.setValue(fieldsOut[i], row.getValue(fieldsIn[i]));
        }
        return next;
    }
}
