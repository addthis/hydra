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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.BundleFormatted;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractBufferOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * <b>args</b>: mergekeycolumn,mergekeycolumn:foldcolumn:foldkey,foldkey:copykeycolumn,copykeycolumn
 * <p/>
 * a fold operation is similar to a merge operation in that it performs a
 * specialized type of merge on rows with contiguous matching (compound) keys.
 * fold differs from merge in that it copies a set of column values associated
 * with a column key.  once all matching rows are found, a single accumulated
 * row is emitted.
 * <p/>
 * for example, fold can turn:
 * <p/>
 * <table border=1>
 * <tr><td>dog<td>red<td>3
 * <tr><td>dog<td>green<td>4
 * <tr><td>cat<td>red<td>1
 * <tr><td>cat<td>green<td>2
 * </table>
 * <p/>
 * into
 * <p/>
 * <table border=1>
 * <tr><td>dog<td>red<td>3<td>green<td>4
 * <tr><td>cat<td>red<td>1<td>green<td>2
 * </table>
 * <p/>
 * where <b>dog/cat</b> are the values of the key column(s),
 * <b>red/green</b> are the values of the fold column and
 * <b>1,2,3,4</b> are the values of the folded column(s).
 *
 * @user-reference
 * @hydra-name fold
 */
public class OpFold extends AbstractBufferOp implements BundleFormatted {

    private final BundleField[] outputFields;
    private final ListBundleFormat format;
    private final String[] keycols;
    private final String foldcol;
    private final String[] foldvals;
    private final String[] copycols;
    private final String[] inputFields;
    private Bundle folded;
    private String lastkey;

    public OpFold(String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        String[] seg = Strings.splitArray(args, ":");
        keycols = Strings.splitArray(seg[0], ",");
        foldcol = seg[1];
        foldvals = Strings.splitArray(seg[2], ",");
        copycols = Strings.splitArray(seg[3], ",");
        List<String> cols = new ArrayList<>(keycols.length + copycols.length + 1);
        // capture input fields
        Collections.addAll(cols, keycols);
        cols.add(foldcol);
        Collections.addAll(cols, copycols);
        inputFields = cols.toArray(new String[cols.size()]);
        // generate output format
        format = new ListBundleFormat();
        outputFields = new BundleField[keycols.length + ((copycols.length + 1) * foldvals.length)];
        for (int i = 0; i < outputFields.length; i++) {
            outputFields[i] = format.getField(Integer.toString(i));
        }
    }

    @Override
    public BundleFormat getFormat() {
        return format;
    }

    @Override
    public List<Bundle> finish() {
        return folded != null ? createRows(folded) : null;
    }

    @Override
    public List<Bundle> next(Bundle row) {
        BundleColumnBinder inputBinder = getSourceColumnBinder(row, inputFields);
        List<Bundle> retval = null;
        String key = "";
        for (int i = 0; i < keycols.length; i++) {
            ValueObject o = inputBinder.getColumn(row, i);
            if (o != null) {
                key = key.concat(o.toString());
            }
        }
        if (folded == null || (lastkey != null && !lastkey.equals(key))) {
            if (folded != null) {
                retval = createRows(folded);
            }
            folded = new ListBundle(format);
            for (BundleField nullField : outputFields) {
                folded.setValue(nullField, null);
            }
            for (int i = 0; i < keycols.length; i++) {
                folded.setValue(outputFields[i], inputBinder.getColumn(row, i));
            }
            for (int i = 0; i < foldvals.length; i++) {
                folded.setValue(outputFields[keycols.length + (i * (copycols.length + 1))], ValueFactory.create(foldvals[i]));
            }
        }
        lastkey = key;
        String foldcolval = inputBinder.getColumn(row, keycols.length).toString();
        for (int i = 0; foldcolval != null && i < foldvals.length; i++) {
            if (foldvals[i].equals(foldcolval)) {
                int offset = keycols.length + (i * (copycols.length + 1));
                for (int j = 0; j < copycols.length; j++) {
                    folded.setValue(outputFields[offset + j + 1], inputBinder.getColumn(row, keycols.length + 1 + j));
                }
                break;
            }
        }
        return retval;
    }

}
