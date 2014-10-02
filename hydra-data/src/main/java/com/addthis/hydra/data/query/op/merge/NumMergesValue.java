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

package com.addthis.hydra.data.query.op.merge;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.Numeric;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.op.MergedRow;

public class NumMergesValue extends AbstractMergedNumber {

    @Override
    public void merge(Bundle nextBundle, MergedRow mergedRow) {
    }

    @Override
    protected Numeric doMerge(Numeric nextValue, Numeric value) {
        throw new UnsupportedOperationException(
                "This method should not be called. Merge(Bundle, MergedRow) should no-op");
    }

    @Override
    public void emit(MergedRow mergedRow) { // skip nul checks
        ValueObject value = ValueFactory.create(mergedRow.getMergedCount());
        mergedRow.setValue(to, value);
    }
}
