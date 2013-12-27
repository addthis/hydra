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

import java.util.Iterator;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractRowOp;


/**
 * <p>This query operation <span class="hydra-summary">removes individual duplicate values</span>.
 * <p>If the values at rows i and i + 1 and column j are identical then the value
 * at (i + 1, j) will be deleted.</p>
 *
 * @user-reference
 * @hydra-name nodup
 */
public class OpNoDup extends AbstractRowOp {

    private Bundle last;

    private static Bundle cloneBundle(Bundle input) {
        Bundle output = input.createBundle();
        Iterator<BundleField> fieldIterator = input.getFormat().iterator();
        while (fieldIterator.hasNext()) {
            BundleField field = fieldIterator.next();
            output.setValue(field, input.getValue(field));
        }
        return output;
    }

    @Override
    public Bundle rowOp(Bundle row) {
        Bundle rowClone = row;
        if (last != null) {
            BundleColumnBinder binder = getSourceColumnBinder(row);
            boolean modified = false;
            for (int i = 0; i < last.getCount() && row.getCount() > i; i++) {
                ValueObject a = binder.getColumn(last, i);
                ValueObject b = binder.getColumn(row, i);
                if (a == null || b == null) {
                    continue;
                }
                if (a.toString().equals(b.toString())) {
                    if (modified == false) {
                        rowClone = cloneBundle(row);
                    }
                    binder.setColumn(row, i, null);
                    modified = true;
                }
            }
        }
        last = rowClone;
        return row;
    }

}
